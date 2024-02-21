package org.jkiss.dbeaver.ext.goldilocks.model.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jkiss.utils.IntKeyMap;

public class GoldilocksPlanBuilder {

    private String plan;

    public GoldilocksPlanBuilder(String plan) {
        this.plan = plan;
    }

    public List<GoldilocksPlanNode> Build() {
        List<GoldilocksPlanNode> rootNodes = new ArrayList<>();
        IntKeyMap<GoldilocksPlanNode> allNodes = new IntKeyMap<>();
        IntKeyMap<GoldilocksPlanNode> levelNodes = new IntKeyMap<>();

        String[] rows = plan.split("\\n");
        int rowCount = rows.length;

        // skip header 4 line
        /*
         * < Execution Plan >
         * ==================================================================================================
         * |  IDX  |  NODE DESCRIPTION                                            |                    ROWS |
         * --------------------------------------------------------------------------------------------------
         */

        int rowPos = 0;
        int idx = 0;
        Matcher matcher = null;
        Pattern pattern = Pattern.compile("\\|\\s+(\\d+)\\s+\\|\\s\\s(.+)\\s+\\|.+\\|");

        for (rowPos = 4; rowPos < rowCount; rowPos++) {
            matcher = pattern.matcher(rows[rowPos]);

            if (matcher.find()) {
                // matcher.group(1) : idx
                // matcher.group(2) : operation
                int level = 0;

                idx = Integer.valueOf(matcher.group(1));
                String operation = matcher.group(2);

                int sPos = 0;
                for (sPos = 0; sPos < operation.length(); sPos++) {
                    if (operation.charAt(sPos) != ' ') {
                        break;
                    }
                }

                if (sPos != 0) {
                    level = sPos / 2;
                }

                operation = operation.trim();

                GoldilocksPlanNode node = new GoldilocksPlanNode(levelNodes, level, idx, operation);

                allNodes.put(idx, node);
                levelNodes.put(level, node);

                if (node.getParent() == null) {
                    rootNodes.add(node);
                }
            } else {
                break;
            }
        }

        // skip header 2 line
        /* ==================================================================================================
         * 
         */

        String detail = "";
        String key = "";
        String value = "";

        pattern = Pattern.compile("\\s+(\\d+)\\s\\s-\\s\\s(.+)");
        for (rowPos += 2; rowPos < rowCount; rowPos++) {
            matcher = pattern.matcher(rows[rowPos]);
            if (matcher.find()) {
                // matcher.group(1) : idx
                // matcher.group(2) : detail
                idx = Integer.valueOf(matcher.group(1));
                detail = matcher.group(2).trim();
            } else {
                detail = rows[rowPos].trim();
            }

            int pos = detail.indexOf(':');

            if (pos >= 0) {
                key = detail.substring(0, pos).trim();
                value = detail.substring(pos + 1).trim();
            } else {
                key = detail;
                value = "";
            }

            GoldilocksPlanNode node = allNodes.get(idx);
            node.addProperty(key, value);
        }

        return rootNodes;
    }
}
