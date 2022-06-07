package com.ververica.cdc.connectors.tdsql.source.split;

import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;

public class TdSqlSplitState {
    private final TdSqlSet setInfo;

    private final MySqlSplitState mySqlSplitState;

    public TdSqlSplitState(TdSqlSet setInfo, MySqlSplitState mySqlSplitState) {
        this.setInfo = setInfo;
        this.mySqlSplitState = mySqlSplitState;
    }

    public TdSqlSet setInfo() {
        return setInfo;
    }

    public MySqlSplitState mySqlSplitState() {
        return mySqlSplitState;
    }

    public MySqlSplit mySqlSplit() {
        return mySqlSplitState.toMySqlSplit();
    }
}
