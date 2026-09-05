package headgateworkflow

import (
	"os"
	"testing"

	"github.com/mujhtech/headgate/go/driver/headgatemysql"
)

func TestWorkflowExperimentsMySQLMatrixCell(t *testing.T) {
	url := os.Getenv("HG_TEST_MYSQL")
	if url == "" {
		t.Skip("HG_TEST_MYSQL not set")
	}
	store, err := headgatemysql.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	runWorkflowMatrixCell(t, store, "mysql")
}
