/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.ms365.client;

import java.util.Collections;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.User;

public class Microsoft365ClientTest extends LastaFluteTestCase {

    private static final Logger logger = LogManager.getLogger(Microsoft365ClientTest.class);

    Microsoft365Client client = null;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);
        if (tenant != null && clientId != null && clientSecret != null) {
            DataStoreParams params = new DataStoreParams();
            params.put(Microsoft365Client.TENANT_PARAM, tenant);
            params.put(Microsoft365Client.CLIENT_ID_PARAM, clientId);
            params.put(Microsoft365Client.CLIENT_SECRET_PARAM, clientSecret);
            client = new Microsoft365Client(params);
        }
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        if (client != null) {
            client.close();
        }
        super.tearDown();
    }

    public void test_getUsers() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.getUsers(Collections.emptyList(), u -> {
            logger.info(ToStringBuilder.reflectionToString(u));
            User user = client.getUser(u.getId(), Collections.emptyList());
            logger.info(ToStringBuilder.reflectionToString(user));
            assertEquals(u.getId(), user.getId());

            client.getNotebookPage(user.getId()).getValue().forEach(n -> {
                logger.info(ToStringBuilder.reflectionToString(n));
            });
        });
    }

    public void test_getGroups() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.getGroups(Collections.emptyList(), g -> {
            logger.info(ToStringBuilder.reflectionToString(g));
            assertNotNull(g.getId());
        });
    }

    public void test_getDrives() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.getDrives(d -> {
            logger.info(ToStringBuilder.reflectionToString(d));
            Drive drive = client.getDrive(d.getId());
            logger.info(ToStringBuilder.reflectionToString(drive));
        });
    }

    public void test_getTeams() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.geTeams(Collections.emptyList(), g -> {
            logger.info(ToStringBuilder.reflectionToString(g));
            assertNotNull(g.getId());
            Group g2 = client.getGroupById(g.getId());
            assertEquals(g.getId(), g2.getId());
            client.getChannels(Collections.emptyList(), c -> {
                logger.info(ToStringBuilder.reflectionToString(c));
                assertNotNull(c.getId());
                Channel c2 = client.getChannelById(g.getId(), c.getId());
                assertEquals(c.getId(), c2.getId());
                client.getTeamMessages(Collections.emptyList(), m -> {
                    logger.info(ToStringBuilder.reflectionToString(m));
                    logger.info(m.getBody().getContentType().toString());
                    logger.info(m.getBody().getContent());
                    client.getTeamReplyMessages(Collections.emptyList(), r -> {
                        logger.info(ToStringBuilder.reflectionToString(r));
                        logger.info(r.getBody().getContentType().toString());
                        logger.info(r.getBody().getContent());
                    }, g.getId(), c.getId(), m.getId());
                }, g.getId(), c.getId());
            }, g.getId());
        });
    }

    public void test_getChats() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        final String chatId = "chat id";
        client.getChatMessages(Collections.emptyList(), m -> {
            logger.info(ToStringBuilder.reflectionToString(m));
            logger.info(m.getBody().getContentType().toString());
            logger.info(m.getBody().getContent());
        }, chatId);
    }
}
