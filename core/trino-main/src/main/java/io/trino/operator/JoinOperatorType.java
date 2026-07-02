/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator;

import io.trino.sql.planner.plan.JoinType;

import static io.trino.operator.join.JoinType.FULL_OUTER;
import static io.trino.operator.join.JoinType.INNER;
import static io.trino.operator.join.JoinType.LOOKUP_OUTER;
import static io.trino.operator.join.JoinType.PROBE_OUTER;
import static java.util.Objects.requireNonNull;

public class JoinOperatorType
{
    private final io.trino.operator.join.JoinType type;
    private final boolean outputSingleMatch;
    private final boolean enforceUniqueMatch;
    private final boolean waitForBuild;

    public static JoinOperatorType ofJoinNodeType(JoinType joinNodeType, boolean outputSingleMatch, boolean enforceUniqueMatch, boolean waitForBuild)
    {
        return switch (joinNodeType) {
            case INNER -> innerJoin(outputSingleMatch, enforceUniqueMatch, waitForBuild);
            case LEFT -> probeOuterJoin(outputSingleMatch, enforceUniqueMatch);
            case RIGHT -> lookupOuterJoin(waitForBuild);
            case FULL -> fullOuterJoin();
        };
    }

    public static JoinOperatorType innerJoin(boolean outputSingleMatch, boolean enforceUniqueMatch, boolean waitForBuild)
    {
        return new JoinOperatorType(INNER, outputSingleMatch, enforceUniqueMatch, waitForBuild);
    }

    public static JoinOperatorType probeOuterJoin(boolean outputSingleMatch, boolean enforceUniqueMatch)
    {
        return new JoinOperatorType(PROBE_OUTER, outputSingleMatch, enforceUniqueMatch, false);
    }

    public static JoinOperatorType lookupOuterJoin(boolean waitForBuild)
    {
        return new JoinOperatorType(LOOKUP_OUTER, false, false, waitForBuild);
    }

    public static JoinOperatorType fullOuterJoin()
    {
        return new JoinOperatorType(FULL_OUTER, false, false, false);
    }

    private JoinOperatorType(io.trino.operator.join.JoinType type, boolean outputSingleMatch, boolean enforceUniqueMatch, boolean waitForBuild)
    {
        this.type = requireNonNull(type, "type is null");
        this.outputSingleMatch = outputSingleMatch;
        this.enforceUniqueMatch = enforceUniqueMatch;
        this.waitForBuild = waitForBuild;
    }

    public boolean isOutputSingleMatch()
    {
        return outputSingleMatch;
    }

    public boolean isEnforceUniqueMatch()
    {
        return enforceUniqueMatch;
    }

    public boolean isWaitForBuild()
    {
        return waitForBuild;
    }

    public io.trino.operator.join.JoinType getType()
    {
        return type;
    }
}
