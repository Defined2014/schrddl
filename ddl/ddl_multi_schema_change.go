package ddl

import (
	"fmt"
	"math/rand"
)

const MaxMultiSchemaChangeStmts int = 5

var supportMultiSchemaChangeDDLKind = []DDLKind{
	ddlAddColumn,
	ddlDropColumn,
	ddlAddIndex,
	ddlDropIndex,
	ddlRenameIndex,
	ddlModifyColumn,
	ddlModifyColumn2,
}

type ddlMultiSchemaChangeJobArg struct {
	subJobs []*ddlJobTask
}

func (c *testCase) multiSchemaChangeJob(task *ddlJobTask) error {
	jobArg := (*ddlMultiSchemaChangeJobArg)(task.arg)
	tblInfo := task.tblInfo

	if c.isTableDeleted(tblInfo) {
		return fmt.Errorf("table %s is not exists", tblInfo.name)
	}

	for _, subJob := range jobArg.subJobs {
		if err := c.updateTableInfo(subJob); err != nil {
			return err
		}
	}
	return nil
}

func (c *testCase) generateMultiSchemaChange(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareMultiSchemaChange, nil, ddlMultiSchemaChange})
	}
	return nil
}

func (c *testCase) prepareMultiSchemaChange(_ interface{}, taskCh chan *ddlJobTask) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	numAlterStmts := rand.Intn(MaxMultiSchemaChangeStmts) + 1

	for i := 0; i < numAlterStmts; i++ {
		ddlKind := supportMultiSchemaChangeDDLKind[rand.Intn(len(supportMultiSchemaChangeDDLKind))]
	}
}
