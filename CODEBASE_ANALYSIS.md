# Microsoft 365 Data Store Connector - Codebase Analysis

## 1. PROJECT OVERVIEW

### Purpose
A powerful Fess Data Store plugin that enables enterprise search across Microsoft 365 services including:
- OneDrive (personal and group drives)
- OneNote (notebooks with sections and pages)
- Teams (channels, messages, chats)
- SharePoint Document Libraries (metadata indexing)
- SharePoint Lists (custom lists and list items)
- SharePoint Pages (site pages and news articles)

### Key Technologies
- **Language**: Java 21+
- **Build**: Maven 3.8+
- **Framework**: Fess Data Store (LastaFlute/DBFlute)
- **API**: Microsoft Graph SDK v6
- **Authentication**: Azure Identity SDK (Client Credentials Flow)
- **Testing**: UTFlute (JUnit 4 based)
- **Dependency Management**: Maven Shade Plugin with Netty relocation

### Version
- Current: 15.3.1-SNAPSHOT
- Latest Release: 15.3.0

---

## 2. PROJECT STRUCTURE

```
src/
├── main/java/org/codelibs/fess/ds/ms365/
│   ├── Microsoft365Constants.java          (110 lines)
│   ├── Microsoft365DataStore.java          (500 lines) - Abstract base class
│   ├── OneDriveDataStore.java              (982 lines)
│   ├── OneNoteDataStore.java               (435 lines)
│   ├── TeamsDataStore.java                 (1027 lines)
│   ├── SharePointDocLibDataStore.java      (546 lines)
│   ├── SharePointListDataStore.java        (776 lines)
│   ├── SharePointPageDataStore.java        (892 lines)
│   └── client/
│       └── Microsoft365Client.java         (1500+ lines)
├── main/resources/
│   └── fess_ds++.xml                       (DI configuration)
└── test/java/org/codelibs/fess/ds/ms365/
    ├── OneDriveDataStoreTest.java
    ├── OneNoteDataStoreTest.java           (320 lines - comprehensive)
    ├── TeamsDataStoreTest.java             (58 lines - minimal)
    ├── SharePointDocLibDataStoreTest.java
    ├── SharePointListDataStoreTest.java    (150+ lines)
    ├── SharePointPageDataStoreTest.java
    └── client/
        └── Microsoft365ClientTest.java
```

**Total Implementation Code**: ~5,268 lines

---

## 3. MAIN IMPLEMENTATION CLASSES

### 3.1 Microsoft365DataStore (Abstract Base Class)
**Location**: `/home/user/fess-ds-microsoft365/src/main/java/org/codelibs/fess/ds/ms365/Microsoft365DataStore.java`

**Purpose**: Provides common functionality for all Microsoft 365 data stores

**Key Methods**:
- `createClient(DataStoreParams)` - Creates Microsoft365Client instance
- `getLicensedUsers(client, consumer)` - Retrieves licensed users with client-side filtering
- `newFixedThreadPool(nThreads)` - Creates thread pool with system resource capping
- `isLicensedUser(client, userId)` - Checks if user has licenses
- `getUserRoles(user)` - Generates Fess roles from user
- `getMicrosoft365Groups(client, consumer)` - Retrieves unified groups
- `getGroupRoles(group)` - Generates Fess roles from group
- `getDriveItemPermissions(client, driveId, item)` - Retrieves file permissions with pagination
- `getSitePermissions(client, siteId)` - Gets site-level permissions
- `assignPermission(client, permissions, permission)` - Converts Microsoft permissions to Fess roles
- `isSystemLibrary(drive)` - Detects system SharePoint libraries
- `isSystemList(list)` - Detects system SharePoint lists
- `getListTemplateType(list)` - Gets SharePoint list template type

**Key Features**:
- Handles permission inheritance and role mapping
- Thread pool management with resource limits
- User/group enumeration with licensing checks
- System list/library filtering

---

### 3.2 OneDriveDataStore
**Location**: `/home/user/fess-ds-microsoft365/src/main/java/org/codelibs/fess/ds/ms365/OneDriveDataStore.java`

**Purpose**: Crawls and indexes files from OneDrive, SharePoint document libraries, and group drives

**Key Methods**:
- `getName()` - Returns "OneDriveDataStore"
- `storeData(dataConfig, callback, paramMap, scriptMap, defaultDataMap)` - Main crawling orchestrator
- `storeSharedDocumentsDrive()` - Crawls authenticated user's OneDrive
- `storeUsersDrive()` - Crawls all licensed users' OneDrive
- `storeGroupsDrive()` - Crawls Microsoft 365 group drives
- `processDriveItem()` - Processes individual files with content extraction
- `getDriveItemContents()` - Extracts file content using Tika
- `getUrl()` - Generates user-friendly URLs for SharePoint/OneDrive
- `getUrlFilter()` - Creates include/exclude pattern filters
- `getCachedUserDriveId()` - Cached drive ID retrieval

**Configuration Parameters**:
```
max_content_length      - File size limit (-1 = unlimited)
ignore_folder          - Skip folder documents (default: true)
supported_mimetypes    - MIME type filter (default: .*)
drive_id              - Specific drive to crawl
shared_documents_drive_crawler - Enable shared docs (default: true)
user_drive_crawler     - Enable user drives (default: true)
group_drive_crawler    - Enable group drives (default: true)
number_of_threads      - Concurrent threads (default: 1)
include_pattern        - Regex inclusion filter
exclude_pattern        - Regex exclusion filter
default_permissions    - Default role assignments
ignore_error          - Continue on errors (default: false)
```

**Metadata Extracted** (30+ fields):
- Basic properties: name, description, size, MIME type, file type
- Timestamps: created, last_modified
- URLs: web_url, webdav_url, processed_url
- Versions: ETag, CTag
- Creator info: user, application, device
- Rich data: image/photo/video metadata, hash values
- Permissions: role-based access control

---

### 3.3 OneNoteDataStore
**Location**: `/home/user/fess-ds-microsoft365/src/main/java/org/codelibs/fess/ds/ms365/OneNoteDataStore.java`

**Purpose**: Crawls and indexes OneNote notebooks with aggregated content from sections and pages

**Key Methods**:
- `getName()` - Returns "OneNoteDataStore"
- `storeData()` - Main crawling orchestrator
- `isSiteNoteCrawler(paramMap)` - Checks if site notebooks enabled
- `isUserNoteCrawler(paramMap)` - Checks if user notebooks enabled
- `isGroupNoteCrawler(paramMap)` - Checks if group notebooks enabled

**Crawling Modes** (Processed in Order):
1. **Site Notebooks**: `/sites/root/onenote/notebooks`
2. **User Notebooks**: `/users/{userId}/onenote/notebooks`
3. **Group Notebooks**: `/groups/{groupId}/onenote/notebooks`

**Configuration Parameters**:
```
site_note_crawler      - Enable site notebooks (default: true)
user_note_crawler      - Enable user notebooks (default: true)
group_note_crawler     - Enable group notebooks (default: true)
number_of_threads      - Concurrent threads (default: 1)
```

**Content Processing Pipeline**:
1. Notebook discovery
2. Section traversal
3. Page HTML content extraction
4. Tika-based text extraction
5. Metadata enrichment

---

### 3.4 TeamsDataStore
**Location**: `/home/user/fess-ds-microsoft365/src/main/java/org/codelibs/fess/ds/ms365/TeamsDataStore.java`

**Purpose**: Crawls and indexes Teams messages, channels, and chats

**Key Methods**:
- `normalizeTextContent()` - Strips HTML and whitespace from message content
- Thread pool management for concurrent message processing

**Crawling Modes**:
- All teams or specific team
- Team filtering by visibility (public/private)
- Specific channels or all channels
- Chat conversations (1:1 or group)

**Configuration Parameters**:
```
team_id               - Specific team to crawl
exclude_team_ids      - Teams to skip (comma-separated)
include_visibility    - Filter by visibility (public, private)
channel_id            - Specific channel to crawl
chat_id              - Specific chat to crawl
ignore_replies        - Skip reply messages (default: false)
append_attachment     - Include attachments (default: true)
ignore_system_events  - Skip system messages (default: true)
title_dateformat      - Date format (default: yyyy/MM/dd'T'HH:mm:ss)
title_timezone_offset - Timezone (default: Z)
number_of_threads     - Concurrent threads (default: 1)
ignore_error         - Continue on errors (default: false)
default_permissions  - Default role assignments
```

**Metadata Extracted**:
- Message properties: id, subject, body, created, modified
- Sender info: from user/application details
- Context: team, channel, parent message
- Interactions: mentions, reactions, importance
- Content: attachments, hosted contents, URLs
- Permissions: role-based access

---

### 3.5 SharePointDocLibDataStore
**Location**: `/home/user/fess-ds-microsoft365/src/main/java/org/codelibs/fess/ds/ms365/SharePointDocLibDataStore.java`

**Purpose**: Indexes SharePoint document libraries as searchable entities (NOT individual files)

**Key Methods**:
- `getName()` - Returns "SharePointDocLibDataStore"
- `storeData()` - Main crawling orchestrator
- `isSystemLibrary(drive)` - Detects system libraries

**Important Note**: Each document library becomes ONE searchable entity with aggregated metadata, not individual files

**Configuration Parameters**:
```
site_id                 - Specific site to crawl
exclude_site_id         - Sites to skip (format: hostname,guid,guid or semicolon-separated)
ignore_system_libraries - Skip system libraries (default: true)
number_of_threads       - Concurrent threads (default: 1)
ignore_error           - Continue on errors (default: false)
include_pattern        - Regex inclusion on library names
exclude_pattern        - Regex exclusion on library names
default_permissions    - Default role assignments
```

**Metadata Aggregated**:
- Library name and description
- Site name and context
- Creation/modification timestamps
- Access permissions
- Standard and canonical URLs

---

### 3.6 SharePointListDataStore
**Location**: `/home/user/fess-ds-microsoft365/src/main/java/org/codelibs/fess/ds/ms365/SharePointListDataStore.java`

**Purpose**: Crawls and indexes SharePoint lists and list items with dynamic field extraction

**Key Methods**:
- `getName()` - Returns "SharePointListDataStore"
- `storeData()` - Main crawling orchestrator
- `getSiteId(paramMap)` - Gets site ID from parameters
- `getListId(paramMap)` - Gets list ID from parameters
- `isExcludedList()` - Checks if list should be skipped
- `isSystemList()` - Detects system lists

**Configuration Parameters**:
```
site_id              - SharePoint site ID (REQUIRED)
list_id              - Specific list to crawl
exclude_list_id      - Lists to skip (comma-separated)
list_template_filter - Filter by template (100=Generic, 101=DocLib, etc.)
ignore_system_lists  - Skip system lists (default: true)
number_of_threads    - Concurrent threads (default: 1)
ignore_error        - Continue on errors (default: false)
include_pattern     - Regex inclusion on item titles
exclude_pattern     - Regex exclusion on item titles
default_permissions - Default role assignments
```

**Content Extraction Strategy**:
- Title: searches Title, LinkTitle, FileLeafRef fields
- Content: aggregates Body, Description, Comments, Notes
- Fields: captures all custom fields in `item.fields` map
- Attachments: processes file attachments on items

**Field Mapping**:
- `item.title` - Item title
- `item.content` - Aggregated content
- `item.fields` - All SharePoint fields as map
- `item.attachments` - File attachments
- `item.created/modified` - Timestamps
- `item.site` - Site metadata (id, name, url)
- `item.list` - List metadata (name, description, url, template_type)

---

### 3.7 SharePointPageDataStore
**Location**: `/home/user/fess-ds-microsoft365/src/main/java/org/codelibs/fess/ds/ms365/SharePointPageDataStore.java`

**Purpose**: Crawls and indexes SharePoint site pages and news articles

**Key Methods**:
- `getName()` - Returns "SharePointPageDataStore"
- `storeData()` - Main crawling orchestrator
- `getSiteId(paramMap)` - Gets site ID
- `isExcludedSite()` - Checks if site should be skipped

**Configuration Parameters**:
```
site_id           - Specific site to crawl
exclude_site_id   - Sites to skip (comma or semicolon-separated)
ignore_system_pages - Skip system pages (default: true)
page_type_filter  - Filter by type (news, article, page)
number_of_threads - Concurrent threads (default: 1)
ignore_error     - Continue on errors (default: false)
include_pattern  - Regex inclusion on page URLs
exclude_pattern  - Regex exclusion on page URLs
default_permissions - Default role assignments
```

**Content Extraction from**:
- Page title
- Page description
- Canvas layout with web parts
- Text web parts and standard web parts
- HTML to plain text conversion

**Page Type Detection**:
- `news` - News posts and announcements
- `article` - Article pages and documentation
- `page` - Standard site pages

---

### 3.8 Microsoft365Client
**Location**: `/home/user/fess-ds-microsoft365/src/main/java/org/codelibs/fess/ds/ms365/client/Microsoft365Client.java`

**Purpose**: Wraps Microsoft Graph SDK v6 with Fess-specific error handling and convenience methods

**Key Responsibilities**:
- Azure AD authentication (Client Credentials Flow)
- GraphServiceClient instance management
- Pagination handling with @odata.nextLink
- Error handling and access exceptions
- Caching of user types and group IDs

**Key Methods** (sampling):
- `getUsers(selectFields, consumer)` - Enumerate users
- `getUser(userId, selectFields)` - Get specific user
- `getUserForLicenseCheck(userId)` - Optimized license check
- `getGroups(selectFields, consumer)` - Enumerate groups
- `getDrives(consumer)` - List available drives
- `getDriveItems(driveId, consumer)` - List drive items
- `getDrivePermissions(driveId, itemId)` - Get file permissions
- `getTeams(consumer)` - List teams
- `getChannels(teamId, consumer)` - List team channels
- `getChatMessages(chatId, consumer)` - Get chat messages
- `getNotebooks(consumer)` - List notebooks
- `getSites(consumer)` - List sites
- `getLists(siteId, consumer)` - List SharePoint lists
- `getListItems(siteId, listId, consumer)` - Get list items
- `getSitePages(siteId, consumer)` - Get site pages

**Caching**:
- User type cache (licensed/guest)
- Group ID cache
- Drive ID lookups with thread-safe lazy initialization

---

## 4. EXISTING JUNIT TESTS

### 4.1 Test Structure Overview

**Framework**: UTFlute (LastaFluteTestCase)
**Test Configuration**: `test_app.xml` with convention and lastaflute includes

### 4.2 Individual Test Files

#### OneDriveDataStoreTest
- **Location**: `/home/user/fess-ds-microsoft365/src/test/java/org/codelibs/fess/ds/ms365/OneDriveDataStoreTest.java`
- **Tests**:
  - `test_getName()` - Verifies data store name
  - `test_getUrl()` - URL generation for different crawler types
  - `test_getUrlFilter()` - URL filter creation

#### OneNoteDataStoreTest (Most Comprehensive)
- **Location**: `/home/user/fess-ds-microsoft365/src/test/java/org/codelibs/fess/ds/ms365/OneNoteDataStoreTest.java`
- **320 Lines of Tests** covering:
  - `test_getName()` - Data store name
  - `test_isGroupNoteCrawler()` - Group notebook crawler flag
  - `test_isUserNoteCrawler()` - User notebook crawler flag
  - `test_isSiteNoteCrawler()` - Site notebook crawler flag
  - `test_numberOfThreads()` - Thread pool configuration
  - `test_notebookConstants()` - Verify constant values
  - `test_crawlerTypeParameters()` - Parameter naming
  - `test_multipleNotebookConfigurations()` - Mixed configurations
  - `test_invalidParameterValues()` - Invalid input handling
  - `test_threadPoolConfiguration()` - Thread pool settings
  - `testStoreData()` - Data storage (commented out)
  - `test_notebookProcessingOrder()` - Crawling order
  - `test_emptyParameterMap()` - Default values
  - `test_caseInsensitiveParameterValues()` - Case handling

#### TeamsDataStoreTest (Minimal)
- **Location**: `/home/user/fess-ds-microsoft365/src/test/java/org/codelibs/fess/ds/ms365/TeamsDataStoreTest.java`
- **58 Lines of Tests** covering:
  - `testNormalizeTextContent()` - HTML/whitespace stripping

#### SharePointDocLibDataStoreTest
- **Location**: `/home/user/fess-ds-microsoft365/src/test/java/org/codelibs/fess/ds/ms365/SharePointDocLibDataStoreTest.java`
- **Tests**:
  - `test_getName()` - Data store name
  - `test_getSiteId()` - Site ID retrieval
  - `test_isExcludedSite()` - Site exclusion filtering

#### SharePointListDataStoreTest
- **Location**: `/home/user/fess-ds-microsoft365/src/test/java/org/codelibs/fess/ds/ms365/SharePointListDataStoreTest.java`
- **150+ Lines of Tests** covering:
  - `test_getName()` - Data store name
  - `test_getSiteId()` - Site ID retrieval
  - `test_getListId()` - List ID retrieval
  - `test_isExcludedList()` - List exclusion filtering
  - `test_isSystemList()` - System list detection
  - `test_isSystemList_withSystemFacet()` - System facet detection

#### SharePointPageDataStoreTest
- **Location**: `/home/user/fess-ds-microsoft365/src/test/java/org/codelibs/fess/ds/ms365/SharePointPageDataStoreTest.java`
- **Tests**:
  - `test_getName()` - Data store name
  - `test_getSiteId()` - Site ID retrieval
  - `test_isExcludedSite()` - Site exclusion filtering

#### Microsoft365ClientTest
- **Location**: `/home/user/fess-ds-microsoft365/src/test/java/org/codelibs/fess/ds/ms365/client/Microsoft365ClientTest.java`
- **Tests** (Integration tests with optional real credentials):
  - `test_getUsers()` - Real user enumeration (requires credentials)
  - `test_getGroups()` - Real group enumeration (requires credentials)
  - Skipped if credentials not provided via environment

---

## 5. KEY FUNCTIONALITY TO TEST

### 5.1 Critical Core Functionality

#### Microsoft365DataStore Base Class
- **Permission Handling**:
  - Pagination through large permission sets with @odata.nextLink
  - Permission conversion to Fess roles (user, group, organization)
  - User and group principal name resolution
  - Permission inheritance from sites

- **User/Group Management**:
  - Licensed user filtering (client-side vs server-side)
  - Unified group enumeration (filtering out non-unified groups)
  - Role assignment generation

- **Thread Pool Management**:
  - System resource cap calculation
  - Proper executor service shutdown
  - Thread pool size validation

- **System Content Filtering**:
  - System library detection (catalogs, style library, forms)
  - System list detection (user information, workflow, etc.)
  - System page filtering

#### OneDriveDataStore
- **Multi-Drive Crawling**:
  - Shared documents drive (current user)
  - User drives (all licensed users)
  - Group drives (Microsoft 365 groups)
  - Specific drive by ID

- **File Processing**:
  - Content extraction with Tika
  - MIME type filtering
  - File size limits
  - Parent folder tracking
  - Folder inclusion/exclusion

- **URL Generation**:
  - SharePoint URLs (Shared Documents path encoding)
  - OneDrive personal URLs
  - Custom drive URLs
  - Proper URL encoding with spaces

- **Permission Extraction**:
  - File-level permissions
  - Pagination handling
  - Role mapping

- **Caching**:
  - Drive ID caching with thread safety
  - Double-checked locking pattern

#### OneNoteDataStore
- **Notebook Discovery**:
  - Site notebooks (root SharePoint)
  - User notebooks (all licensed users)
  - Group notebooks (Microsoft 365 groups)

- **Content Aggregation**:
  - Section enumeration
  - Page content extraction (HTML)
  - Tika-based text extraction
  - Content size calculation

- **Parameter Handling**:
  - Case-insensitive boolean parsing
  - Null/missing parameter defaults
  - Invalid value handling

- **Configuration Modes**:
  - Selective crawler enabling
  - Mixed configurations
  - Processing order verification

#### TeamsDataStore
- **Message Extraction**:
  - Team enumeration
  - Channel enumeration
  - Message content extraction
  - HTML content normalization
  - Attachment handling

- **Filtering**:
  - Team ID filtering
  - Visibility filtering (public/private)
  - Reply message filtering
  - System event filtering

- **Metadata**:
  - Message titles with date formatting
  - Timezone offset handling
  - Sender information
  - Mention extraction

- **Threading**:
  - Concurrent message processing
  - Thread pool management

#### SharePointDocLibDataStore
- **Library Enumeration**:
  - All sites crawling
  - Specific site crawling
  - Site exclusion (simple and complex IDs)

- **Library Filtering**:
  - System library detection
  - Include/exclude pattern matching
  - Library template type

- **URL Generation**:
  - Standard Documents library URLs
  - Custom library URL encoding
  - Graph API URL preservation

- **Metadata Aggregation**:
  - Library name and description
  - Site context
  - Creation/modification tracking

#### SharePointListDataStore
- **List Discovery**:
  - All lists in site
  - Specific list by ID
  - List exclusion
  - Template-based filtering

- **System List Handling**:
  - System facet detection
  - Name-based detection fallback
  - Web URL-based detection

- **List Item Processing**:
  - Title extraction (Title, LinkTitle, FileLeafRef)
  - Content aggregation (Body, Description, Comments)
  - Dynamic field mapping
  - Field expansion when needed

- **Attachment Support**:
  - Attachment detection
  - Attachment metadata
  - Content integration

- **Filtering**:
  - Include/exclude patterns on item titles

#### SharePointPageDataStore
- **Page Discovery**:
  - All sites vs specific site
  - Page type filtering (news, article, page)
  - Site exclusion

- **Content Extraction**:
  - Page title
  - Page description
  - Canvas layout parsing
  - Web part content
  - HTML to text conversion

- **Filtering**:
  - System page filtering
  - URL pattern matching

#### Microsoft365Client
- **Authentication**:
  - Client credentials flow
  - Token caching/refresh
  - Credential validation

- **Pagination**:
  - @odata.nextLink handling
  - Collection response processing
  - Large result set pagination

- **Error Handling**:
  - API exception handling
  - Access denied exceptions
  - Quota/throttling handling
  - Retry logic

- **Caching**:
  - User type caching
  - Group ID caching
  - Cache invalidation

---

### 5.2 Integration Points

- **Fess Framework Integration**:
  - DataConfig and DataStoreParams handling
  - IndexUpdateCallback usage
  - FailureUrlService integration
  - PermissionHelper integration
  - CrawlerStatsHelper usage
  - ComponentUtil dependency injection

- **Microsoft Graph SDK**:
  - Request building and execution
  - Response model mapping
  - Pagination iterator patterns
  - Field selection/expansion

- **Tika Content Extraction**:
  - Extractor registration and lookup
  - Content type handling
  - Text extraction reliability

---

### 5.3 Error Handling Scenarios

- **API Errors**:
  - 404 Not Found (deleted items)
  - 403 Forbidden (permission denied)
  - 429 Too Many Requests (throttling)
  - 500 Server Error (temporary issues)
  - Network timeouts

- **Crawling Resilience**:
  - `ignore_error` parameter behavior
  - Partial failure handling
  - Exception aggregation (MultipleCrawlingAccessException)
  - Statistics tracking

- **Resource Management**:
  - Thread pool shutdown
  - Client connection cleanup
  - Executor service termination
  - Interrupt handling

---

### 5.4 Performance Considerations

- **Threading**:
  - Correct thread pool sizing
  - System resource cap effectiveness
  - Thread interruption handling
  - Executor shutdown timeouts

- **API Efficiency**:
  - Field selection optimization
  - Pagination efficiency
  - Cache hit rates
  - Duplicate API call prevention

- **Memory Management**:
  - Large file handling
  - Pagination memory usage
  - Thread pool memory overhead

---

## 6. ARCHITECTURAL PATTERNS

### 6.1 Template Method Pattern
- `Microsoft365DataStore` defines abstract `storeData()` method
- Each subclass implements specific crawling logic

### 6.2 Factory Pattern
- `createClient()` method creates `Microsoft365Client` instances

### 6.3 Consumer/Callback Pattern
- Heavy use of `Consumer<T>` for pagination
- `IndexUpdateCallback` for document indexing

### 6.4 Strategy Pattern
- URL filters with include/exclude patterns
- Crawler type selection (shared, user, group, specific)

### 6.5 Caching Pattern
- User type and group ID caching
- Drive ID caching with thread-safe initialization
- Double-checked locking in OneDriveDataStore

### 6.6 Thread Pool Pattern
- ExecutorService with configurable thread count
- System resource cap calculation
- Proper lifecycle management

---

## 7. TESTING RECOMMENDATIONS

### High Priority (Critical Functionality)
1. Permission handling with pagination
2. Multi-drive crawling (shared, user, group)
3. Content extraction and MIME type filtering
4. Thread pool management and resource capping
5. System library/list detection
6. OneNote content aggregation
7. Teams message extraction and HTML normalization
8. SharePoint list item field extraction
9. URL generation and encoding
10. Pagination with @odata.nextLink

### Medium Priority (Important Features)
1. Parameter validation and defaults
2. Error handling and ignore_error behavior
3. Include/exclude pattern matching
4. Attachment processing
5. Role-based access control mapping
6. Timezone offset handling
7. Boolean parameter parsing (case-insensitive)
8. Statistics tracking

### Low Priority (Nice to Have)
1. Cache invalidation
2. Performance optimization
3. Executor shutdown timeouts
4. Thread interruption handling

