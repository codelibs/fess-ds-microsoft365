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
package org.codelibs.fess.ds.ms365;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.opensearch.config.exentity.CrawlingConfig;
import org.codelibs.fess.opensearch.config.exentity.FailureUrl;
import org.codelibs.fess.util.ComponentUtil;

/**
 * A {@link FailureUrlService} that records what it was handed instead of writing to OpenSearch.
 *
 * <p>The real service needs a {@code FailureUrlBhv} bound to a live OpenSearch, so
 * {@code ComponentUtil.getComponent(FailureUrlService.class)} could not resolve anything at all
 * in a unit test and the failure-handling helpers on {@link Microsoft365DataStore} ran in no
 * test. This stub is declared in {@code test_app.xml} so the container resolves it by class,
 * which lets those helpers execute and lets a test assert over the arguments they passed rather
 * than merely over "nothing was thrown".</p>
 *
 * <p>The container is shared by every test class in this module, so the recorded calls survive
 * across classes: call {@link #clear()} before exercising a failure path.</p>
 */
public class CapturingFailureUrlService extends FailureUrlService {

    /**
     * One recorded {@link #store(CrawlingConfig, String, String, Throwable)} call.
     *
     * @param crawlingConfig the config the failure was recorded against
     * @param errorName the recorded error name
     * @param url the value used as the failure-URL row key
     * @param throwable the throwable whose stack trace would have been stored
     */
    public record StoredFailure(CrawlingConfig crawlingConfig, String errorName, String url, Throwable throwable) {
    }

    private final List<StoredFailure> storedFailures = Collections.synchronizedList(new ArrayList<>());

    /**
     * Default constructor.
     */
    public CapturingFailureUrlService() {
        super();
    }

    @Override
    public FailureUrl store(final CrawlingConfig crawlingConfig, final String errorName, final String url, final Throwable e) {
        storedFailures.add(new StoredFailure(crawlingConfig, errorName, url, e));
        return null;
    }

    /**
     * Resolves the stub the container holds and forgets whatever earlier tests recorded on it.
     *
     * <p>Asserting the type here is deliberate: it pins that {@code test_app.xml} really is what
     * satisfies {@code ComponentUtil.getComponent(FailureUrlService.class)} inside the failure
     * handlers. Without that registration the lookup throws and the handlers cannot run at all,
     * which is how they came to have no executing coverage.</p>
     *
     * @return the container's stub, with no recorded calls.
     */
    public static CapturingFailureUrlService empty() {
        final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
        if (!(failureUrlService instanceof final CapturingFailureUrlService capturing)) {
            throw new IllegalStateException("test_app.xml must resolve FailureUrlService to " + CapturingFailureUrlService.class.getName()
                    + ", got " + failureUrlService.getClass().getName());
        }
        capturing.clear();
        return capturing;
    }

    /**
     * @return the calls recorded so far, oldest first.
     */
    public List<StoredFailure> getStoredFailures() {
        synchronized (storedFailures) {
            return new ArrayList<>(storedFailures);
        }
    }

    /**
     * Forgets every recorded call.
     */
    public void clear() {
        storedFailures.clear();
    }
}
