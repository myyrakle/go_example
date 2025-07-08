package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func SetupPostgreSQLConnection() *sql.DB {
	// Postgre
	// SQL 연결 설정
	connStr := "host=? user=? password=? dbname=? sslmode=disable"
	postgresqlConnection, err := sql.Open("postgres", connStr)

	if err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}

	// 연결 테스트
	if err := postgresqlConnection.Ping(); err != nil {
		log.Fatalf("failed to ping PostgreSQL: %v", err)
	}

	fmt.Println("PostgreSQL connection established successfully")

	return postgresqlConnection
}

type PeekWALChangesResponse struct {
	PgOutputs PgOutputs
	LastLSN   string
}

func PeekWALChanges(postgresqlConnection *sql.DB, slotName string, publicationName string, limit uint64) (PeekWALChangesResponse, error) {
	response := PeekWALChangesResponse{
		PgOutputs: make(PgOutputs, 0),
	}

	query := `
		SELECT lsn, xid, data 
		FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, 'proto_version', '1', 'publication_names', $3)
	`

	rows, err := postgresqlConnection.Query(query, slotName, limit, publicationName)
	if err != nil {
		return response, fmt.Errorf("failed to peek WAL changes: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var lsn string
		var xid string
		var data []byte // 바이너리 데이터이므로 []byte 사용
		if err := rows.Scan(&lsn, &xid, &data); err != nil {
			return response, fmt.Errorf("failed to scan WAL change: %v", err)
		}

		response.LastLSN = lsn

		if len(data) == 0 {
			continue
		}

		buf := bytes.NewReader(data)

		// 메시지 타입 읽기 (첫 번째 바이트)
		var messageType byte
		if err := binary.Read(buf, binary.BigEndian, &messageType); err != nil {
			continue
		}

		switch MessageType(messageType) {
		case PgOutputMessageInsert:
			pgOutput, err := parsePgOutput(postgresqlConnection, buf, MessageType(messageType))
			if err != nil {
				return response, fmt.Errorf("failed to parse INSERT message: %v", err)
			}
			response.PgOutputs = append(response.PgOutputs, pgOutput)
		case PgOutputMessageUpdate:
			pgOutput, err := parsePgOutput(postgresqlConnection, buf, MessageType(messageType))
			if err != nil {
				return response, fmt.Errorf("failed to parse UPDATE message: %v", err)
			}
			response.PgOutputs = append(response.PgOutputs, pgOutput)
		case PgOutputMessageDelete:
			pgOutput, err := parsePgOutput(postgresqlConnection, buf, MessageType(messageType))
			if err != nil {
				return response, fmt.Errorf("failed to parse DELETE message: %v", err)
			}
			response.PgOutputs = append(response.PgOutputs, pgOutput)
		case PgOutputMessageBegin:
			response.PgOutputs = append(response.PgOutputs, PgOutput{
				MessageType: PgOutputMessageBegin,
			})
			continue
		case PgOutputMessageCommit:
			response.PgOutputs = append(response.PgOutputs, PgOutput{
				MessageType: PgOutputMessageCommit,
			})
			continue
		case PgOutputMessageTruncate:
			response.PgOutputs = append(response.PgOutputs, PgOutput{
				MessageType: PgOutputMessageTruncate,
			})
			continue
		case PgOutputMessageRelation:
			response.PgOutputs = append(response.PgOutputs, PgOutput{
				MessageType: PgOutputMessageRelation,
			})
			continue
		default:
			continue
		}
	}

	if err := rows.Err(); err != nil {
		return response, fmt.Errorf("error iterating over rows: %v", err)
	}

	return response, nil
}

func AdvanceReplicationSlot(postgresqlConnection *sql.DB, slotName string, lsn string) error {
	query := `
		SELECT pg_replication_slot_advance($1, $2)
	`
	_, err := postgresqlConnection.Exec(query, slotName, lsn)
	if err != nil {
		return fmt.Errorf("failed to advance replication slot: %v", err)
	}
	return nil
}

func main() {
	postgresqlConnection := SetupPostgreSQLConnection()

	totalMessageCount := 0
	changedMessageCount := 0
	for {
		peekPgOutputsResponse, err := PeekWALChanges(postgresqlConnection, "test_slot", "test_publication", 65536)
		if err != nil {
			log.Fatalf("failed to peek WAL changes: %v", err)
		}

		if len(peekPgOutputsResponse.PgOutputs) == 0 {
			log.Println("No changes found in the WAL.")

			log.Printf("Total messages: %d, Changed messages: %d", totalMessageCount, changedMessageCount)
			time.Sleep(10 * time.Second) // Wait before the next peek
			continue
		}

		for _, pgOutput := range peekPgOutputsResponse.PgOutputs {
			totalMessageCount++

			switch pgOutput.MessageType {
			case PgOutputMessageInsert:
				changedMessageCount++
				log.Printf("Insert: Relation ID: %d, Payload: %+v", pgOutput.RelationID, pgOutput.Payload)
			case PgOutputMessageUpdate:
				changedMessageCount++
				log.Printf("Update: Relation ID: %d, Payload: %+v", pgOutput.RelationID, pgOutput.Payload)
			case PgOutputMessageDelete:
				changedMessageCount++
				log.Printf("Delete: Relation ID: %d, Payload: %+v", pgOutput.RelationID, pgOutput.Payload)
			}
		}

		if peekPgOutputsResponse.LastLSN != "" {
			if err := AdvanceReplicationSlot(postgresqlConnection, "test_slot", peekPgOutputsResponse.LastLSN); err != nil {
				log.Fatalf("failed to advance replication slot: %v", err)
			}
			log.Printf("Advanced replication slot to LSN: %s", peekPgOutputsResponse.LastLSN)
		}
	}
}

type MessageType byte

const (
	PgOutputMessageBegin    MessageType = 'B'
	PgOutputMessageCommit   MessageType = 'C'
	PgOutputMessageOrigin   MessageType = 'O'
	PgOutputMessageRelation MessageType = 'R'
	PgOutputMessageType     MessageType = 'Y'
	PgOutputMessageInsert   MessageType = 'I'
	PgOutputMessageUpdate   MessageType = 'U'
	PgOutputMessageDelete   MessageType = 'D'
	PgOutputMessageTruncate MessageType = 'T'
)

type PgTupleType uint8

const (
	PgTupleTypeNew PgTupleType = 'N' // New tuple (after INSERT or UPDATE)
)

type PgOutput struct {
	MessageType
	RelationID uint32
	TupleType  PgTupleType
	Payload    map[string]any
}

type PgOutputs []PgOutput

var tableDefinitionMap map[uint32]TableDefinition
var tableDefinitionMapMutex sync.Mutex

type TableDefinition struct {
	TableName string
	Columns   ColumnDefinitions
}

type ColumnDefinition struct {
	Index      int
	ColumnName string
	DataType   string
	Nullable   bool
}

type ColumnDefinitions []ColumnDefinition

func (cd ColumnDefinitions) FindByIndex(index int) *ColumnDefinition {
	for _, column := range cd {
		if column.Index == index {
			return &column
		}
	}
	return nil
}

func GetTableDefinition(connection *sql.DB, relationID uint32) (TableDefinition, error) {
	tableDefinitionMapMutex.Lock()
	defer tableDefinitionMapMutex.Unlock()

	if tableDefinitionMap == nil {
		tableDefinitionMap = make(map[uint32]TableDefinition)
	}

	if tableDef, exists := tableDefinitionMap[relationID]; exists {
		return tableDef, nil
	}

	query := `
		SELECT relname 
		FROM pg_catalog.pg_class 
		WHERE oid = $1
	`

	var tableName string
	err := connection.QueryRow(query, relationID).Scan(&tableName)
	if err != nil {
		if err == sql.ErrNoRows {
			return TableDefinition{}, fmt.Errorf("table with relation ID %d not found", relationID)
		}
		return TableDefinition{}, fmt.Errorf("failed to get table name: %v", err)
	}

	query = `
		SELECT 
			a.attname AS column_name,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
			a.attnotnull AS not_null,
			a.attnum AS position,
			a.atthasdef AS has_default
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1
		AND a.attnum > 0
		AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	rows, err := connection.Query(query, relationID)
	if err != nil {
		return TableDefinition{}, fmt.Errorf("failed to get table columns: %v", err)
	}
	defer rows.Close()

	columns := make(ColumnDefinitions, 0)
	for rows.Next() {
		var columnName, dataType string
		var notNull bool
		var position int
		var hasDefault bool

		if err := rows.Scan(&columnName, &dataType, &notNull, &position, &hasDefault); err != nil {
			return TableDefinition{}, fmt.Errorf("failed to scan row: %v", err)
		}

		column := ColumnDefinition{
			Index:      position,
			ColumnName: columnName,
			DataType:   dataType,
			Nullable:   !notNull,
		}
		columns = append(columns, column)
	}

	tableDefinition := TableDefinition{
		TableName: tableName,
		Columns:   columns,
	}

	tableDefinitionMap[relationID] = tableDefinition

	return tableDefinition, nil
}

func parsePgOutput(postgresqlConnection *sql.DB, buf *bytes.Reader, messageType MessageType) (PgOutput, error) {
	pgOutput := PgOutput{
		Payload:     make(map[string]any),
		MessageType: messageType,
	}

	if err := binary.Read(buf, binary.BigEndian, &pgOutput.RelationID); err != nil {
		return PgOutput{}, fmt.Errorf("failed to read relation ID: %w", err)
	}
	if err := binary.Read(buf, binary.BigEndian, &pgOutput.TupleType); err != nil {
		return PgOutput{}, fmt.Errorf("failed to read tuple type: %w", err)
	}

	var columnCount uint16
	if err := binary.Read(buf, binary.BigEndian, &columnCount); err != nil {
		return PgOutput{}, fmt.Errorf("failed to read column count: %w", err)
	}

	tableDefinition, err := GetTableDefinition(postgresqlConnection, pgOutput.RelationID)
	if err != nil {
		return PgOutput{}, fmt.Errorf("failed to get table definition: %w", err)
	}

	for i := uint16(0); i < columnCount; i++ {
		var columnType uint8
		if err := binary.Read(buf, binary.BigEndian, &columnType); err != nil {
			return PgOutput{}, fmt.Errorf("failed to read column type: %w", err)
		}

		index := int(i + 1)

		columnDefinition := tableDefinition.Columns.FindByIndex(index)
		if columnDefinition == nil {
			return PgOutput{}, fmt.Errorf("column with index %d not found in table definition", index)
		}

		switch columnType {
		case 'n': // NULL value
			pgOutput.Payload[columnDefinition.ColumnName] = nil
		case 'u': // UNCHANGED value (for UPDATE)
		case 't': // Text value
			var length uint32
			if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
				return PgOutput{}, fmt.Errorf("failed to read text length: %w", err)
			}

			value := make([]byte, length)
			if err := binary.Read(buf, binary.BigEndian, value); err != nil {
				return PgOutput{}, fmt.Errorf("failed to read text value: %w", err)
			}

			pgOutput.Payload[columnDefinition.ColumnName] = string(value)
		case 'b': // Binary value
			var length uint32
			if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
				return PgOutput{}, fmt.Errorf("failed to read binary length: %w", err)
			}

			value := make([]byte, length)
			if err := binary.Read(buf, binary.BigEndian, value); err != nil {
				return PgOutput{}, fmt.Errorf("failed to read binary value: %w", err)
			}

			pgOutput.Payload[columnDefinition.ColumnName] = value
		default:
			return PgOutput{}, fmt.Errorf("unknown column type: %c", columnType)
		}
	}

	return pgOutput, nil
}
