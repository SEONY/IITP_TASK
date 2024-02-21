package org.jkiss.dbeaver.ext.goldilocks.model;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndex;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.meta.PropertyLength;
import org.jkiss.dbeaver.model.struct.rdb.DBSIndexType;

public class GoldilocksIndex extends GenericTableIndex {

    private String indexType;
    private String tablespace;
    private String description;

    public GoldilocksIndex(GoldilocksTable table, boolean nonUnique, String qualifier, long cardinality,
            String indexName, boolean persisted, String tablespace, String indexType, String description) {
        super(table, nonUnique, qualifier, cardinality, indexName, DBSIndexType.OTHER, persisted);
        this.tablespace = tablespace;
        this.description = description;
        this.indexType = indexType;
    }

    @Property(viewable = true, order = 0)
    public String getName() {
        return super.getName();
    }

    @Property(viewable = true, order = 3)
    public String getGoldilocksIndexType() {
        return this.indexType;
    }

    @Property(viewable = true, order = 5)
    public String getTablespace() {
        return tablespace;
    }

    @Nullable
    @Override
    @Property(viewable = true, length = PropertyLength.MULTILINE, order = 100)
    public String getDescription() {
        return description;
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context) {
        return DBUtils.getFullQualifiedName(getDataSource(), getTable().getCatalog(), getTable().getSchema(), this);
    }

    @Property(hidden = true)
    public String getQualifier() {
        return super.getQualifier();
    }

    @Property(hidden = true)
    public long getCardinality() {
        return super.getCardinality();
    }

    @Property(hidden = true)
    public DBSIndexType getIndexType() {
        return super.getIndexType();
    }
}
