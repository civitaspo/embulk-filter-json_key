package org.embulk.filter.json_key;

import com.google.common.base.Throwables;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * Created by takahiro.nakayama on 10/28/15.
 */
class ColumnVisitorImpl
        implements ColumnVisitor
{
    private final Logger logger = Exec.getLogger(ColumnVisitorImpl.class);
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final String filterColumnName;
    private final JsonKeyFilter filter;

    public ColumnVisitorImpl(PageReader pageReader, PageBuilder pageBuilder, JsonKeyFilter filter, String filterColumnName)
    {
        this.pageReader = pageReader;
        this.pageBuilder = pageBuilder;
        this.filterColumnName = filterColumnName;
        this.filter = filter;
    }

    @Override
    public void booleanColumn(Column outputColumn) {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(outputColumn));
        }
    }

    @Override
    public void longColumn(Column outputColumn) {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setLong(outputColumn, pageReader.getLong(outputColumn));
        }
    }

    @Override
    public void doubleColumn(Column outputColumn) {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setDouble(outputColumn, pageReader.getDouble(outputColumn));
        }
    }

    @Override
    public void stringColumn(Column outputColumn) {
        if (outputColumn.getName().contentEquals(filterColumnName)) {
            String output = null;
            try {
                output = filter.doFilter(pageReader.getString(outputColumn));
            }
            catch (IOException e) {
                Throwables.propagate(e);
            }

            if (output == null) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setString(outputColumn, output);
            }
        }
        else {
            if (pageReader.isNull(outputColumn)) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setString(outputColumn, pageReader.getString(outputColumn));
            }
        }
    }

    @Override
    public void timestampColumn(Column outputColumn) {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setTimestamp(outputColumn, pageReader.getTimestamp(outputColumn));
        }
    }
}
