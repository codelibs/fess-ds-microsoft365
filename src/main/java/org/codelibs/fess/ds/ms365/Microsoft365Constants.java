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

import java.util.Map;
import java.util.Set;

/**
 * Constants used across Microsoft 365 data store implementations.
 *
 * @author shinsuke
 */
public final class Microsoft365Constants {

    private Microsoft365Constants() {
        // Utility class - prevent instantiation
    }

    // Default values
    /** Default value used when the list template type is unknown. */
    public static final String UNKNOWN_TEMPLATE = "unknown";
    /** SharePoint list template type for document libraries. */
    public static final String DOCUMENT_LIBRARY = "documentLibrary";
    /** SharePoint list template type for generic lists. */
    public static final String GENERIC_LIST = "genericList";

    /**
     * Graph template names of the libraries a list crawl can reach, i.e. the lists whose forms live
     * under {@code Forms/}. Limited to names Graph has been seen to report.
     */
    private static final Set<String> LIBRARY_TEMPLATES = Set.of(DOCUMENT_LIBRARY, "pictureLibrary", "webPageLibrary");

    /** SharePoint list template type IDs mapped to the template names Graph reports. */
    private static final Map<String, String> TEMPLATE_NAMES_BY_ID =
            Map.of("100", GENERIC_LIST, "101", DOCUMENT_LIBRARY, "102", "survey", "103", "links", "104", "announcements", "105", "contacts",
                    "106", "events", "108", "discussionBoard", "109", "pictureLibrary", "171", "tasksWithTimelineAndHierarchy");

    /**
     * Translates a SharePoint list template ID to the template name Graph reports.
     *
     * <p>Only the IDs Microsoft documents against Graph's {@code list.template}, and those whose
     * name Graph has been seen to report, are mapped. Other IDs exist in SharePoint's own
     * SPListTemplateType enumeration (e.g. 107, the legacy Tasks list), but guessing at their
     * names would recreate the silent no-match this mapping exists to fix.</p>
     *
     * @param id the numeric template ID
     * @return the Graph template name, or null if the ID has no documented mapping
     */
    public static String templateNameForId(final String id) {
        return TEMPLATE_NAMES_BY_ID.get(id);
    }

    /**
     * Checks if a Graph template name is that of a document library, picture library or site pages
     * library.
     *
     * @param template the Graph template name
     * @return true if lists of this template are libraries
     */
    public static boolean isLibraryTemplate(final String template) {
        return LIBRARY_TEMPLATES.contains(template);
    }
}
