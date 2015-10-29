package org.embulk.filter.json_key;

import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import static org.embulk.filter.json_key.JsonKeyFilterPlugin.*;

/**
 * Created by takahiro.nakayama on 10/28/15.
 */
public class FilteredPageOutput
        implements PageOutput
{
    private final Logger logger = Exec.getLogger(FilteredPageOutput.class);
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final PageOutput pageOutput;
    private final Schema outputSchema;
    private final ColumnVisitorImpl columnVisitor;

    public FilteredPageOutput(PluginTask task, Schema inputSchema, Schema outputSchema, PageOutput pageOutput)
    {
        this.pageReader = new PageReader(inputSchema);
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
        this.pageOutput = pageOutput;
        this.outputSchema = outputSchema;
        this.columnVisitor = new ColumnVisitorImpl(pageReader, pageBuilder, new JsonKeyFilter(task), task.getColumnName());
    }

    @Override
    public void add(Page page)
    {
        pageReader.setPage(page);

        while (pageReader.nextRecord()) {
            outputSchema.visitColumns(columnVisitor);
            pageBuilder.addRecord();
        }
    }

    @Override
    public void finish()
    {
        pageBuilder.finish();
        pageOutput.finish();
    }

    @Override
    public void close()
    {
        pageReader.close();
        pageBuilder.close();
        pageOutput.close();
    }
}
