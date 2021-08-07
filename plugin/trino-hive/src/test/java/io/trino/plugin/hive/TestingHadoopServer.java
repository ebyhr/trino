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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;

import static java.lang.String.format;

public class TestingHadoopServer
        implements Closeable
{
    private static final Logger log = Logger.get(TestingHadoopServer.class);
    private static final String HOST_NAME = "hadoop-master";

    private final GenericContainer<?> dockerContainer;

    public TestingHadoopServer()
    {
        dockerContainer = new GenericContainer<>(DockerImageName.parse("ghcr.io/trinodb/testing/hdp3.1-hive:41"))
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOST_NAME))
                .withExposedPorts(1180, 9083)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new HostPortWaitStrategy());
        dockerContainer.setPortBindings(ImmutableList.of("9000:9000"));
        dockerContainer.start();
    }

    public boolean checkFileOnHdfs(String filePath)
    {
        try {
            Container.ExecResult result = dockerContainer.execInContainer("hdfs", "dfs", "-test", "-e", filePath);
            return result.getExitCode() == 0;
        }
        catch (IOException | InterruptedException e) {
            log.warn("Failed to check existence of HDFS file: " + filePath, e);
            return false;
        }
    }

    public String copyFileToContainer(byte[] bytes, String serverPath)
    {
        dockerContainer.copyFileToContainer(Transferable.of(bytes), serverPath);
        try {
            Container.ExecResult result = dockerContainer.execInContainer("hdfs", "dfs", "-copyFromLocal", serverPath, serverPath);
            if (result.getExitCode() != 0) {
                log.error(result.getStderr());
            }
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to copy file to HDFS: " + serverPath, e);
        }
        return format("hdfs://%s:9000/%s", HOST_NAME, serverPath);
    }

    public String createHdfsDirectory(String directory)
    {
        try {
            dockerContainer.execInContainer("hdfs", "dfs", "-mkdir", directory);
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to create HDFS directory: " + directory, e);
        }
        return format("hdfs://%s:9000%s", HOST_NAME, directory);
    }

    public boolean deleteHdfsDirectory(String hdfsPath)
    {
        try {
            Container.ExecResult result = dockerContainer.execInContainer("hdfs", "dfs", "-rm", "-R", hdfsPath);
            return result.getExitCode() == 0;
        }
        catch (IOException | InterruptedException e) {
            log.warn("Failed to delete HDFS files: " + hdfsPath, e);
            return false;
        }
    }

    public boolean deleteHdfsFile(String hdfsPath)
    {
        try {
            Container.ExecResult result = dockerContainer.execInContainer("hdfs", "dfs", "-rm", hdfsPath);
            return result.getExitCode() == 0;
        }
        catch (IOException | InterruptedException e) {
            log.warn("Failed to delete HDFS files: " + hdfsPath, e);
            return false;
        }
    }

    public String getSocksProxy()
    {
        return format("%s:%s", HOST_NAME, dockerContainer.getMappedPort(1180));
    }

    public String getThriftUri()
    {
        return format("thrift://%s:%s", HOST_NAME, dockerContainer.getMappedPort(9083));
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
