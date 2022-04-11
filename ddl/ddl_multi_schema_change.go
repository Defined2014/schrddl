package ddl

import (
	"fmt"
	"math/rand"
	"strings"
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

type multiSchemaChangeCtx struct {
	tblInfo    *ddlTestTable
	sql        string
	modifyCols map[string]struct{}
	modifyIdx  map[string]struct{}
	arg        *ddlMultiSchemaChangeJobArg
}

func modifiableColumn(ctx *multiSchemaChangeCtx, col *ddlTestColumn) bool {
	if _, ok := ctx.modifyCols[col.name]; ok {
		return false
	}
	ctx.modifyCols[col.name] = struct{}{}
	return true
}

func modifiableIndex(ctx *multiSchemaChangeCtx, idx *ddlTestIndex) bool {
	if _, ok := ctx.modifyIdx[idx.name]; ok {
		return false
	}
	ctx.modifyIdx[idx.name] = struct{}{}
	return true
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

func (c *testCase) prepareSubJobs(ctx *multiSchemaChangeCtx, ddlKind DDLKind) error {
	tmpCh := make(chan *ddlJobTask, 1)
	table := ctx.tblInfo
	prefixSQL := strings.ToLower(fmt.Sprintf("ALTER TABLE `%s` ", table.name))
	switch ddlKind {
	case ddlAddColumn:
		if err := c.prepareAddColumn(ctx, tmpCh); err != nil {
			return err
		}
	case ddlDropColumn:
		if err := c.prepareDropColumn(ctx, tmpCh); err != nil {
			return err
		}
	case ddlAddIndex:
		if err := c.prepareAddIndex(ctx, tmpCh); err != nil {
			return err
		}
	case ddlDropIndex:
		if err := c.prepareDropIndex(ctx, tmpCh); err != nil {
			return err
		}
	case ddlRenameIndex:
		if err := c.prepareRenameIndex(ctx, tmpCh); err != nil {
			return err
		}
	case ddlModifyColumn:
		if err := c.prepareModifyColumn(ctx, tmpCh); err != nil {
			return err
		}
	case ddlModifyColumn2:
		if err := c.prepareModifyColumn2(ctx, tmpCh); err != nil {
			return err
		}
	}
	subJob := <-tmpCh
	pos := strings.Index(strings.ToLower(subJob.sql), prefixSQL)
	if pos != -1 {
		if len(ctx.arg.subJobs) != 0 {
			ctx.sql += ", "
		}
		ctx.sql += subJob.sql[pos+len(prefixSQL):]
	} else {
		return fmt.Errorf("invalid sub job sql: %s", subJob.sql)
	}
	ctx.arg.subJobs = append(ctx.arg.subJobs, subJob)
	close(tmpCh)
	return nil
}

func (c *testCase) prepareMultiSchemaChange(_ interface{}, taskCh chan *ddlJobTask) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	numAlterStmts := rand.Intn(MaxMultiSchemaChangeStmts) + 1

	ctx := &multiSchemaChangeCtx{
		tblInfo: table,
		sql:     fmt.Sprintf("ALTER TABLE `%s` ", table.name),
		arg: &ddlMultiSchemaChangeJobArg{
			subJobs: make([]*ddlJobTask, 0, numAlterStmts),
		},
	}
	for i := 0; i < numAlterStmts; i++ {
		ddlKind := supportMultiSchemaChangeDDLKind[rand.Intn(len(supportMultiSchemaChangeDDLKind))]
		if err := c.prepareSubJobs(ctx, ddlKind); err != nil {
			return err
		}
	}

	task := &ddlJobTask{
		k:       ddlMultiSchemaChange,
		sql:     ctx.sql,
		tblInfo: table,
		arg:     ddlJobArg(ctx.arg),
	}
	taskCh <- task
	return nil
}
