package org.jkiss.dbeaver.ext.goldilocks;

import org.jkiss.dbeaver.ext.generic.GenericDataSourceProvider;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.connection.DBPDriver;
import org.jkiss.utils.CommonUtils;

public class GoldilocksDataSourceProvider extends GenericDataSourceProvider {
    
    @Override
    public String getConnectionURL(DBPDriver driver, DBPConnectionConfiguration connectionInfo) {
        StringBuilder url = new StringBuilder();
        url.append("jdbc:goldilocks://").append(connectionInfo.getHostName());
        if (!CommonUtils.isEmpty(connectionInfo.getHostPort())) {
            url.append(":").append(connectionInfo.getHostPort());
        }
        if (!CommonUtils.isEmpty(connectionInfo.getDatabaseName())) {
            url.append("/").append(connectionInfo.getDatabaseName());
        }
        return url.toString();
    }
}
