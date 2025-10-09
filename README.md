# Microsoft365 Data Store for Fess

[![Java CI with Maven](https://github.com/codelibs/fess-ds-microsoft365/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-microsoft365/actions/workflows/maven.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.codelibs.fess/fess-ds-microsoft365.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.codelibs.fess%22%20AND%20a:%22fess-ds-microsoft365%22)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A powerful Fess Data Store plugin that enables enterprise search across Microsoft 365 services including OneDrive, OneNote, Teams, SharePoint Document Libraries, SharePoint Lists, and SharePoint Pages via Microsoft Graph API v6.

## Overview

This plugin extends [Fess](https://fess.codelibs.org/) enterprise search capabilities to comprehensively index Microsoft 365 content, providing unified search across your organization's cloud documents, conversations, and data with role-based access control integration.

## ✨ Key Features

### 📁 **Comprehensive Content Crawling**
- **OneDrive**: User drives, group drives, shared documents, and specific drives with metadata extraction
- **OneNote**: Complete notebooks with aggregated content from all sections and pages, supporting site, user, and group notebooks
- **Teams**: Channels, messages, chats with conversation context
- **SharePoint Document Libraries**: Document library metadata indexing (libraries crawled as searchable entities, not individual files)
- **SharePoint Lists**: Custom lists and list items with dynamic field mapping
- **SharePoint Pages**: Site pages, news articles, and wiki pages with full content extraction

### 🔐 **Enterprise-Grade Security**
- **Role-based Access Control**: Seamless integration with Fess security model
- **Azure AD Authentication**: Client credentials flow with automatic token refresh
- **Permission Inheritance**: Preserves Microsoft 365 access permissions in search results

### ⚡ **Performance & Reliability**
- **Microsoft Graph SDK v6**: Latest API with efficient pagination and caching
- **Multi-threaded Processing**: Configurable thread pools for optimal performance
- **Smart Caching**: Drive ID, user type, and group ID caching to reduce API calls
- **Robust Error Handling**: Comprehensive error tracking with configurable failure recovery
- **Content Filtering**: Advanced include/exclude patterns with system content filtering

### 🛠 **Developer-Friendly**
- **Maven Integration**: Clean build process with dependency shading
- **Extensive Testing**: UTFlute-based test framework with mock Graph API responses
- **Configurable Field Mapping**: Customizable data extraction scripts for each service

## 🚀 Quick Start

### Prerequisites

- **Java**: 21 or higher
- **Fess**: 15.2.0 or higher
- **Azure AD**: App registration with Microsoft Graph API permissions

### Installation

#### Option 1: Download Pre-built JAR
1. Download the latest `fess-ds-microsoft365-X.X.X.jar` from [Maven Central](https://repo1.maven.org/maven2/org/codelibs/fess/fess-ds-microsoft365/)
2. Copy the JAR file to your Fess installation:
   ```bash
   # For standard installation
   cp fess-ds-microsoft365-X.X.X.jar $FESS_HOME/app/WEB-INF/lib/
   
   # For system-wide installation
   sudo cp fess-ds-microsoft365-X.X.X.jar /usr/share/fess/app/WEB-INF/lib/
   ```
3. Restart Fess service

#### Option 2: Build from Source
```bash
# Clone the repository
git clone https://github.com/codelibs/fess-ds-microsoft365.git
cd fess-ds-microsoft365

# Build the plugin
mvn clean package

# Copy to Fess installation
cp target/fess-ds-microsoft365-*.jar $FESS_HOME/app/WEB-INF/lib/
```

### Azure App Registration Setup

Before using this plugin, create an Azure App registration with the required permissions:

1. **Register Application** in Azure Portal
2. **Add API Permissions** (Microsoft Graph):
   - `Files.Read.All` - OneDrive file access
   - `Sites.Read.All` - SharePoint sites and lists
   - `Notes.Read.All` - OneNote notebooks
   - `Chat.Read.All` - Teams chat messages
   - `ChannelMessage.Read.All` - Teams channel messages
   - `ChannelMember.Read.All` - Teams channel member list
   - `Team.ReadBasic.All` - Teams basic information
   - `User.Read.All` - User directory access
   - `Group.Read.All` - Group information
3. **Grant Admin Consent** for the permissions
4. **Create Client Secret** and note the values

### Basic Configuration

Configure the following authentication parameters in Fess:

```properties
# Required Azure AD credentials
tenant=********-****-****-****-************
client_id=********-****-****-****-************  
client_secret=***********************

# Optional performance settings
number_of_threads=1
ignore_error=false
```

## 📊 Data Store Types

The plugin provides six specialized data store types, each optimized for different Microsoft 365 services:

| Data Store | Service | Content Types | Use Cases |
|------------|---------|---------------|----------|
| `oneDriveDataStore` | OneDrive | Files, Folders, Metadata | Document search, file discovery |
| `oneNoteDataStore` | OneNote | Notebooks (with sections & pages content) | Knowledge base search, note finding, documentation search |
| `teamsDataStore` | Teams | Channels, Messages, Chats | Conversation search, team communication |
| `sharePointDocLibDataStore` | SharePoint | Document Libraries (metadata only) | Document library discovery and metadata search |
| `sharePointListDataStore` | SharePoint | Lists, List Items, Custom Fields | Structured data search, business process content |
| `sharePointPageDataStore` | SharePoint | Site Pages, News Articles, Wiki Pages | Web content search, intranet portal search |

### Configuration in Fess Admin UI

1. Navigate to **System > Data Store**
2. Click **Create New**
3. Select the desired data store type from the dropdown
4. Configure authentication and crawling parameters
5. Save and start crawling

### Scripts

#### OneDrive

```
title=file.name
content=file.description + "\n" + file.contents
mimetype=file.mimetype
created=file.created
last_modified=file.last_modified
url=file.web_url
role=file.roles
```

| Key | Value |
| --- | --- |
| file.name | The name of the file. |
| file.description | A short description of the file. |
| file.contents | The text contents of the file (extracted using Tika) |
| file.mimetype | The MIME type of the file. |
| file.filetype | The file type category determined by MIME type. |
| file.created | The time at which the file was created. |
| file.last_modified | The last time the file was modified by anyone. |
| file.size | The size of the file in bytes. |
| file.web_url | A link for opening the file in an editor or viewer in a browser. |
| file.url | The processed URL for the file (may differ from web_url for certain locations). |
| file.roles | Users/groups who can access the file. |
| file.ctag | Change tag for the file (used for change tracking). |
| file.etag | Entity tag for the file (used for caching). |
| file.id | The unique identifier of the file in OneDrive. |
| file.webdav_url | WebDAV URL for the file (if available). |
| file.location | Geographic location metadata (if available). |
| file.createdby_application | Application that created the file. |
| file.createdby_device | Device that created the file. |
| file.createdby_user | User who created the file. |
| file.deleted | Deletion information (if file was deleted). |
| file.hashes | File hash values for integrity checking. |
| file.last_modifiedby_application | Application that last modified the file. |
| file.last_modifiedby_device | Device that last modified the file. |
| file.last_modifiedby_user | User who last modified the file. |
| file.image | Image metadata (for image files). |
| file.parent | Parent reference information. |
| file.parent_id | ID of the parent folder. |
| file.parent_name | Name of the parent folder. |
| file.parent_path | Path to the parent folder. |
| file.photo | Photo metadata (for photo files). |
| file.publication | Publication information (if applicable). |
| file.search_result | Search result metadata (if file was found via search). |
| file.special_folder | Special folder name (if file is in a special folder). |
| file.video | Video metadata (for video files). |

#### OneNote

```
title=notebook.name
content=notebook.contents
created=notebook.created
last_modified=notebook.last_modified
url=notebook.web_url
role=notebook.roles
size=notebook.size
```

| Key | Value |
| --- | --- |
| notebook.name | The name of the notebook. |
| notebook.contents | The extracted text contents from all sections and pages within the notebook. |
| notebook.size | The size of the notebook content in characters. |
| notebook.created | The time at which the notebook was created. |
| notebook.last_modified | The last time the notebook was modified by anyone. |
| notebook.web_url | A link for opening the notebook in OneNote web or desktop app. |
| notebook.roles | Users/groups who can access the notebook. |

#### Teams

```
title=message.title
content=message.content
created=message.created_date_time
last_modified=message.last_modified_date_time
url=message.web_url
role=message.roles
```

| Key | Value |
| --- | --- |
| message.title | The message title (sender name and timestamp). |
| message.content | The text contents of the message including attachments if configured. |
| message.created_date_time | The time at which the message was created. |
| message.last_modified_date_time | The last time the message was modified. |
| message.web_url | A link for opening the message in Teams. |
| message.roles | Users/groups who can access the team/channel/chat. |
| message.id | The unique identifier of the message. |
| message.from | The sender information. |
| message.subject | The subject of the message. |
| message.body | The body content with type information. |
| message.attachments | File attachments associated with the message. |
| message.mentions | Users mentioned in the message. |
| team | The team object containing team information (when applicable). |
| channel | The channel object containing channel information (when applicable). |
| parent | The parent message for replies (when applicable). |

#### SharePoint Document Libraries

```
title=doclib.name
content=doclib.content
created=doclib.created
last_modified=doclib.modified
url=doclib.url
role=doclib.roles
```

| Key | Value |
| --- | --- |
| doclib.id | The unique identifier of the document library (Drive ID). |
| doclib.name | The name of the document library. |
| doclib.description | The description of the document library. |
| doclib.content | Rich content combining document library name, description, and site name for enhanced search. |
| doclib.web_url | The Microsoft Graph API web URL for the document library. |
| doclib.url | The standardized SharePoint URL for accessing the document library. |
| doclib.created | The time at which the document library was created. |
| doclib.modified | The last time the document library was modified. |
| doclib.type | The type of the drive (e.g., "documentLibrary"). |
| doclib.site_name | The display name of the SharePoint site containing this document library. |
| doclib.site_url | The web URL of the SharePoint site. |
| doclib.roles | Users/groups who can access the document library. |

**Important**: SharePointDocLibDataStore indexes document libraries themselves as searchable entities (not the files within them). Each document library becomes one search result containing aggregated metadata including library name, description, and parent site information. For individual file indexing within SharePoint document libraries, use the OneDriveDataStore which handles SharePoint document library files through the Microsoft Graph Drive API.

#### SharePoint Lists

```
title=item.title
content=item.content
created=item.created
last_modified=item.modified
url=item.url
role=item.roles
```

| Key | Value |
| --- | --- |
| item.title | The title of the list item (extracted from Title, LinkTitle, or FileLeafRef fields). |
| item.content | The text contents of the list item (extracted from Body, Description, Comments, or Notes fields) |
| item.id | The unique identifier of the list item |
| item.created | The time at which the list item was created. |
| item.modified | The last time the list item was modified. |
| item.url | A link for opening the list item in SharePoint. |
| item.fields | All fields and values from the SharePoint list item as a map |
| item.attachments | File attachments associated with the list item (if any) |
| item.roles | Users/groups who can access the list item. |
| item.site | Site information containing `id`, `name`, and `url` |
| item.list | List information containing `name`, `description`, `url`, and `template_type` |

**Data Structure**: The `item` object contains nested structures:
- `item.site` - Contains site metadata (site.id, site.name, site.url)
- `item.list` - Contains list metadata (list.name, list.description, list.url, list.template_type)
- `item.fields` - Dynamic map of all SharePoint list fields and their values

**Note**: The plugin automatically expands SharePoint list item fields to ensure content extraction. If fields are not initially available, it performs an individual API call with `$expand=fields` to retrieve the complete field data.

#### SharePoint Pages

```
title=page.title
content=page.content
created=page.created
last_modified=page.modified
url=page.url
role=page.roles
```

| Key | Value |
| --- | --- |
| page.title | The title of the SharePoint page. |
| page.content | The extracted text content from the page canvas layout including web parts. |
| page.id | The unique identifier of the page. |
| page.created | The time at which the page was created. |
| page.modified | The last time the page was modified. |
| page.author | The user who created the page. |
| page.type | The type of page (news, article, wiki, page). |
| page.description | The page description or summary. |
| page.url | A link for opening the page in SharePoint. |
| page.canonical_url | The canonical URL for accessing the page. |
| page.promotion_state | The promotion status of the page (for news pages). |
| page.site_name | The display name of the SharePoint site containing this page. |
| page.site_url | The web URL of the SharePoint site. |
| page.roles | Users/groups who can access the page. |

**Content Extraction**: The SharePointPageDataStore extracts content from:
- **Page Title**: The main page title
- **Page Description**: Page description or summary text
- **Canvas Layout**: Text content from web parts (TextWebPart, StandardWebPart)
- **Web Parts**: HTML content converted to plain text with proper formatting

**Page Types**: The plugin automatically detects and categorizes pages:
- `news`: News posts and announcements
- `article`: Article pages and documentation
- `wiki`: Wiki-style collaborative pages
- `page`: Standard site pages

**Note**: Content extraction from canvas layout depends on the Microsoft Graph SDK's ability to retrieve web part data. The plugin handles both text web parts and attempts to extract meaningful content from standard web parts when possible.

## ⚙️ Configuration Reference

### Authentication Parameters (Required)

| Parameter | Description | Example |
|-----------|-------------|----------|
| `tenant` | Azure AD tenant ID | `contoso.onmicrosoft.com` or GUID |
| `client_id` | App registration client ID | `12345678-1234-1234-1234-123456789abc` |
| `client_secret` | App registration client secret | `abcdefghijk...` |

### Common Crawling Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|----------|
| `number_of_threads` | Concurrent crawling threads | `1` | `3` |
| `ignore_error` | Continue on errors | `true` | `false` |
| `include_pattern` | Regex pattern for inclusion | - | `.*\.pdf$` |
| `exclude_pattern` | Regex pattern for exclusion | - | `.*temp.*` |
| `default_permissions` | Default role assignments | - | `{role}admin` |

### Teams-Specific Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `team_id` | Specific team ID to crawl | All teams | Microsoft 365 group ID |
| `exclude_team_ids` | Comma-separated team IDs to exclude | - | Multiple teams to skip |
| `include_visibility` | Team visibility levels to include | All | Comma-separated: `public`, `private` |
| `channel_id` | Specific channel ID to crawl | All channels | Within specified team |
| `chat_id` | Specific chat ID to crawl | - | For 1:1 or group chats |
| `ignore_replies` | Skip reply messages | `false` | Process only root messages |
| `append_attachment` | Include attachments in content | `true` | Append attachment text to message body |
| `ignore_system_events` | Skip system event messages | `true` | Filter out system notifications |
| `title_dateformat` | Date format for message titles | `yyyy/MM/dd'T'HH:mm:ss` | Java date pattern |
| `title_timezone` | Timezone for message titles | `Z` | e.g., `UTC`, `Asia/Tokyo`, `America/New_York` |
| `number_of_threads` | Number of processing threads | `1` | Concurrent message processing |
| `default_permissions` | Default role assignments | - | Additional permissions for all messages |
| `ignore_error` | Continue crawling on errors | `false` | Set to `true` to skip failed messages |

#### Teams Implementation Details

The TeamsDataStore provides comprehensive Microsoft Teams content crawling with the following capabilities:

**Core Functionality:**
- **Team-based Crawling**: Processes teams, channels, and messages hierarchically
- **Chat Support**: Crawls 1:1 and group chat conversations when chat_id is specified
- **Message Aggregation**: Consolidates chat messages into searchable conversation threads
- **Permission Mapping**: Extracts team/channel membership and maps to Fess role-based access control

**Crawling Modes:**
- **All Teams**: Leave `team_id` empty to crawl all accessible teams
- **Specific Team**: Set `team_id` to crawl only that team's channels and messages
- **Team Filtering**: Use `exclude_team_ids` to skip specific teams (comma-separated IDs)
- **Visibility Filtering**: Use `include_visibility` to filter by team visibility (public/private)
- **Specific Channel**: Set both `team_id` and `channel_id` to crawl a single channel
- **Chat Conversations**: Set `chat_id` to crawl specific chat conversations

**Content Processing:**
- **Message Title Generation**: Creates searchable titles using sender name and formatted timestamp
- **Content Extraction**: Extracts message body content (text/HTML) with proper formatting
- **Attachment Handling**: Optionally includes attachment information in message content
- **Reply Threading**: Supports crawling of reply messages with parent message context
- **System Event Filtering**: Automatically filters out system-generated messages

**Message Metadata Fields:**
The implementation extracts comprehensive message metadata including:
- Basic properties: id, subject, body, created/modified timestamps
- Sender information: from user/application details
- Conversation context: team, channel, parent message references
- Interaction data: mentions, reactions, importance level
- Rich content: attachments, hosted contents, web URLs
- Permission data: role-based access control from team/channel membership

**Performance Optimizations:**
- **Multi-threaded Processing**: Configurable thread pool for parallel message processing
- **Efficient Pagination**: Uses Microsoft Graph PageIterator for handling large message sets
- **Selective Field Expansion**: Expands only necessary fields to reduce API calls
- **Permission Caching**: Caches group membership data to optimize permission mapping

**Error Handling & Resilience:**
- **Configurable Error Handling**: `ignore_error` parameter controls continuation on failures
- **Comprehensive Logging**: Debug and info level logging for monitoring progress
- **Thread Pool Management**: Proper executor service shutdown and cleanup
- **Interruption Handling**: Graceful handling of thread interruption

**Content Filtering:**
- **Reply Message Filtering**: Option to skip reply messages and process only root messages
- **System Event Filtering**: Automatic detection and filtering of system-generated events
- **URL Pattern Matching**: Support for include/exclude patterns on message content

**Use Cases:**
- **Team Communication Search**: Find conversations across teams and channels
- **Knowledge Discovery**: Search team discussions for solutions and decisions
- **Compliance Monitoring**: Index team communications for compliance requirements
- **Chat History Search**: Search through direct and group chat conversations

**Crawling Modes**:
- **Shared Documents Drive**: Enable `shared_documents_drive_crawler` to crawl the current user's OneDrive
- **User Drives**: Enable `user_drive_crawler` to crawl all licensed users' OneDrive
- **Group Drives**: Enable `group_drive_crawler` to crawl Microsoft 365 group drives
- **Specific Drive**: Set `drive_id` to crawl only that specific drive

### OneNote-Specific Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `site_note_crawler` | Enable crawling of site notebooks | `true` | Crawls notebooks at the root SharePoint site |
| `user_note_crawler` | Enable crawling of user notebooks | `true` | Crawls personal OneNote notebooks for licensed users |
| `group_note_crawler` | Enable crawling of group notebooks | `true` | Crawls shared notebooks in Microsoft 365 groups |
| `number_of_threads` | Number of processing threads | `1` | Controls concurrent notebook processing |

#### OneNote Implementation Details

The OneNoteDataStore provides comprehensive OneNote notebook crawling with the following implementation features:

**Core Functionality:**
- **Multi-Source Notebook Crawling**: Processes notebooks from three distinct sources in a systematic order
- **Aggregated Content Extraction**: Consolidates all sections and pages within each notebook into searchable content
- **Permission Mapping**: Extracts notebook access permissions and maps them to Fess role-based access control

**Crawling Modes (Processing Order):**
1. **Site Notebooks**: Crawls notebooks at the root SharePoint site level (`/sites/root/onenote/notebooks`)
2. **User Notebooks**: Iterates through all licensed users and crawls their personal notebooks (`/users/{userId}/onenote/notebooks`)
3. **Group Notebooks**: Crawls shared notebooks associated with Microsoft 365 groups (`/groups/{groupId}/onenote/notebooks`)

**Content Processing Pipeline:**
1. **Notebook Discovery**: Uses Microsoft Graph API to enumerate notebooks based on enabled crawling modes
2. **Section Traversal**: For each notebook, retrieves all sections within it
3. **Page Content Extraction**: For each section, fetches all pages and extracts their HTML content
4. **Content Aggregation**: Combines all page content using Tika to extract plain text from HTML
5. **Metadata Enrichment**: Captures notebook metadata including creation/modification times and access URLs

**Configuration Flexibility:**
- **Selective Crawling**: Enable/disable specific notebook sources independently
- **Boolean Parameter Handling**: Case-insensitive boolean values (`true`, `True`, `TRUE`, `false`, `False`, `FALSE`)
- **Invalid Value Handling**: Invalid boolean values default to `false` for safety
- **Null Value Handling**: Null or missing parameters use default values (all crawlers enabled by default)

**Performance Optimizations:**
- **Concurrent Processing**: Configurable thread pool for parallel notebook processing
- **Efficient API Usage**: Batches API calls where possible to reduce Graph API quota consumption
- **Content Size Tracking**: Monitors and reports content size for each notebook

**Error Handling & Resilience:**
- **Graceful Degradation**: Handles invalid parameter values by defaulting to safe configurations
- **Thread Pool Management**: Proper executor service lifecycle management with shutdown handling
- **Comprehensive Logging**: Debug-level logging for monitoring crawling progress and troubleshooting

**Content Metadata Fields:**
The implementation extracts and indexes the following notebook metadata:
- `notebook.name`: The display name of the notebook
- `notebook.contents`: Aggregated text content from all sections and pages
- `notebook.size`: Total size of the extracted content in characters
- `notebook.created`: Notebook creation timestamp
- `notebook.last_modified`: Last modification timestamp
- `notebook.web_url`: Direct link to open the notebook in OneNote
- `notebook.roles`: Users/groups with access permissions

**Use Cases:**
- **Knowledge Base Search**: Search across organizational OneNote documentation
- **Personal Note Discovery**: Find information in personal OneNote notebooks
- **Team Collaboration Search**: Search shared team notebooks for meeting notes and project documentation
- **Cross-Platform Content**: Index OneNote content created from web, desktop, and mobile applications

### OneDrive-Specific Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `max_content_length` | Maximum content length in bytes | `-1` (unlimited) | Set size limit for file content |
| `ignore_folder` | Skip folder documents | `true` | Process files only, ignore folders |
| `supported_mimetypes` | Supported MIME types pattern | `.*` | Regex pattern for supported file types |
| `drive_id` | Specific drive ID to crawl | - | If specified, only crawls this drive |
| `shared_documents_drive_crawler` | Enable shared documents crawling | `true` | Crawl default user's OneDrive |
| `user_drive_crawler` | Enable user drives crawling | `true` | Crawl all licensed users' drives |
| `group_drive_crawler` | Enable group drives crawling | `true` | Crawl Microsoft 365 group drives |

#### OneDrive Implementation Details

The OneDriveDataStore provides comprehensive Microsoft 365 file crawling capabilities with the following implementation features:

**Core Functionality:**
- **Multi-Drive Type Support**: Processes files from OneDrive personal drives, SharePoint document libraries (via Drive API), and Microsoft 365 group drives
- **Hierarchical File Traversal**: Recursively crawls drive items starting from root, handling both files and folders with proper parent-child relationships
- **Content Extraction & Indexing**: Each file becomes a searchable entity with extracted content, metadata, and permission information
- **Permission Integration**: Extracts and maps Microsoft 365 access permissions to Fess role-based access control

**Crawling Modes (Processing Order):**
1. **Shared Documents Drive**: Crawls the authenticated user's OneDrive (`/me/drive`) or all SharePoint sites' document libraries
2. **User Drives**: Iterates through all licensed users and crawls their personal OneDrive (`/users/{userId}/drive`)
3. **Group Drives**: Crawls Microsoft 365 group-associated drives (`/groups/{groupId}/drive`)
4. **Specific Drive**: Targets a single drive by ID when `drive_id` parameter is specified (`/drives/{driveId}`)

**Content Processing Pipeline:**
1. **Drive Discovery**: Uses Microsoft Graph API to enumerate drives based on enabled crawling modes and site/drive access
2. **Item Enumeration**: Retrieves drive items using pagination with `DriveItemCollectionResponse` and `@odata.nextLink` handling
3. **Content Filtering**: Applies MIME type filtering, file size limits, and include/exclude patterns before processing
4. **Content Extraction**: Uses Tika extractor with configurable name (`extractorName`, default: "tikaExtractor") for text extraction from supported file types
5. **Metadata Enrichment**: Extracts comprehensive file metadata including timestamps, permissions, and parent folder information
6. **URL Generation**: Creates user-friendly URLs based on crawler type and SharePoint/OneDrive location patterns

**Performance Optimizations:**
- **Drive ID Caching**: Thread-safe caching of user drive IDs using double-checked locking pattern (`cachedUserDriveId` with `driveIdCacheLock`)
- **Concurrent Processing**: Configurable thread pool (`number_of_threads`) for parallel processing of multiple drives and files
- **Efficient Pagination**: Handles Microsoft Graph API pagination using `@odata.nextLink` with helper methods
- **Smart Filtering**: Pre-filters items by MIME type patterns and file size before expensive content extraction
- **Interruption Handling**: Proper detection and handling of thread interruption during long-running operations

**Error Handling & Resilience:**
- **Configurable Error Tolerance**: `ignore_error` parameter controls whether to continue crawling on individual item failures
- **Exception Classification**: Differentiates between access exceptions and general exceptions for appropriate error handling
- **Failure URL Tracking**: Integration with Fess failure URL service for monitoring and retry capabilities
- **Comprehensive Logging**: Debug-level logging for detailed crawling progress monitoring and troubleshooting

**Content Metadata Extraction:**
The implementation extracts and indexes 30+ metadata fields per file:
- **Basic Properties**: name, description, size, MIME type, file type, creation/modification timestamps
- **Location & Access**: web URLs, WebDAV URLs, processed URLs for SharePoint navigation
- **Version Control**: ETag, CTag for change detection and synchronization
- **Creator Information**: user, application, and device details for created/modified by tracking
- **Rich Metadata**: image/photo/video properties, geographic location data, file hash values
- **Folder Structure**: parent reference information including path, name, and ID
- **Specialized Data**: publication info, search result metadata, special folder classification
- **Permission Data**: role-based access control extracted from Microsoft Graph permissions API

**URL Processing Strategy:**
The implementation generates user-friendly URLs based on crawling context:
- **SharePoint Libraries**: `{siteUrl}/Shared%20Documents/{path}` for shared/group drives
- **OneDrive Personal**: `{siteUrl}/Documents/{path}` for user drives
- **Custom Drives**: `{siteUrl}/{driveName}/{path}` for specific drive crawling
- **URL Encoding**: Proper encoding of file and folder names with space handling

**Content Size Management:**
- **Configurable Limits**: `max_content_length` parameter with fallback to Fess content length helper
- **MIME Type Support**: Regex pattern matching for `supported_mimetypes` (default: all types)
- **Folder Handling**: Optional folder document creation controlled by `ignore_folder` parameter
- **Size Validation**: Pre-extraction validation to avoid processing oversized files

### SharePoint Document Library Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `site_id` | Specific site ID to crawl | All sites | Full site ID format: `hostname,siteCollectionId,siteId` |
| `exclude_site_id` | Site IDs to exclude | - | See format guide below |
| `ignore_system_libraries` | Skip system libraries | `true` | Excludes Form Templates, Style Library, etc. |
| `number_of_threads` | Number of processing threads | `1` | Concurrent document library processing |
| `ignore_error` | Continue crawling on errors | `false` | Set to `true` to skip failed libraries |
| `include_pattern` | Regex pattern for library names to include | - | Filter libraries by name matching |
| `exclude_pattern` | Regex pattern for library names to exclude | - | Skip libraries with matching names |
| `default_permissions` | Default role assignments | - | Additional permissions for all libraries |

##### exclude_site_id Format

SharePoint site IDs contain commas as part of their format (`hostname,siteCollectionId,siteId`). To properly exclude sites:

- **Single SharePoint site**: Use the full site ID as-is
  ```
  exclude_site_id=site1.sharepoint.com,686d3f1a-a383-4367-b5f5-93b99baabcf3,12048306-4e53-420e-bd7c-31af611f6d8a
  ```

- **Multiple SharePoint sites**: Separate with semicolons (`;`)
  ```
  exclude_site_id=site1.sharepoint.com,guid1,guid1;site2.sharepoint.com,guid2,guid2
  ```

- **Legacy simple IDs**: Comma-separated (for backward compatibility)
  ```
  exclude_site_id=site1,site2,site3
  ```

#### SharePoint Document Library Implementation Details

The SharePointDocLibDataStore provides comprehensive metadata indexing for SharePoint document libraries across your organization with the following key features:

**Core Functionality:**
- **Library-Level Indexing**: Each SharePoint document library becomes a single searchable entity in the Fess index, combining library metadata with site context for enhanced discoverability
- **Site Traversal**: Supports crawling all accessible sites or targeting specific sites using the `site_id` parameter
- **System Library Filtering**: Automatically excludes system document libraries (Form Templates, Style Library, etc.) unless explicitly configured otherwise with `ignore_system_libraries` parameter
- **Permission Integration**: Extracts and maps SharePoint access permissions to Fess role-based access control

**Content Aggregation:**
The implementation creates rich, searchable content by combining:
- Document library name and description
- Parent SharePoint site name and context
- Library creation and modification timestamps
- Access permission information
- Standardized URLs for both Graph API access and user navigation

**URL Generation Strategy:**
- **Graph API URL**: Preserved from Microsoft Graph response for API compatibility (`doclib.web_url`)
- **Canonical URL**: Generated standardized SharePoint URLs for user navigation (`doclib.url`)
  - Standard "Documents" libraries: `{siteUrl}/Shared%20Documents`
  - Custom libraries: `{siteUrl}/{encodedLibraryName}` with proper URL encoding

**Multi-Threading Support:**
- Configurable concurrent processing using `number_of_threads` parameter (default: 1)
- Thread-safe execution with proper ExecutorService management and resource cleanup
- Graceful handling of thread interruption during long-running operations
- 60-second timeout for executor shutdown with forced termination as fallback

**Error Handling & Resilience:**
- Comprehensive error tracking with integration into Fess failure URL service
- Configurable error handling via `ignore_error` parameter (default: false)
- Detailed logging for monitoring and troubleshooting crawling operations
- Statistical tracking for performance monitoring and reporting using CrawlerStatsHelper
- Distinction between CrawlingAccessException and general exceptions for appropriate handling

**Performance Optimizations:**
- Efficient site and drive enumeration using Microsoft Graph API with pagination support
- Drive type filtering to process only document library drives (`documentLibrary` type)
- Parallel processing of multiple document libraries within sites using ExecutorService
- Memory-efficient processing with proper resource management and cleanup

**Configuration Flexibility:**
- **Site Exclusion**: Advanced `exclude_site_id` parameter supporting both simple comma-separated IDs and complex SharePoint site ID format with semicolon separation
- **Pattern Filtering**: Support for `include_pattern` and `exclude_pattern` regex filtering on library names
- **Permission Management**: Default permissions assignment via `default_permissions` parameter
- **Threading Control**: Configurable `number_of_threads` for optimal performance tuning

**Integration with Fess Security Model:**
- Automatic extraction of SharePoint permissions using Microsoft Graph API drive permissions endpoint
- Conversion of Microsoft 365 access permissions to Fess role format with proper encoding
- Support for default permission assignment via configuration parameters
- Inheritance of site-level permissions for document libraries with pagination support for large permission sets

**Use Cases:**
- **Document Library Discovery**: Find and access specific SharePoint document libraries across the organization
- **Content Organization**: Search for document libraries by name, description, or site context
- **Permission Auditing**: Identify document libraries and their access permissions
- **Site Navigation**: Discover available document libraries within SharePoint sites

**Important Note**: This data store focuses on document library metadata indexing. For indexing individual files within SharePoint document libraries, use the OneDriveDataStore which handles SharePoint document library files through the Microsoft Graph Drive API.

### SharePoint List Parameters

| Parameter | Description | Default | Notes |
| --- | --- | --- | --- |
| `site_id` | SharePoint site ID containing lists | Required | Full site ID format: `hostname,siteCollectionId,siteId` |
| `list_id` | Specific list ID to crawl | All lists | If specified, only this list will be crawled |
| `exclude_list_id` | Comma-separated list IDs to exclude | - | Multiple list IDs separated by commas |
| `list_template_filter` | Filter by list template types | - | Comma-separated template IDs (e.g., `100,101`) |
| `ignore_system_lists` | Skip system lists | `true` | Excludes lists like User Information, Workflow Tasks |
| `ignore_error` | Continue crawling on errors | `false` | Set to `true` to skip failed items |
| `include_pattern` | Regex pattern for item titles to include | - | Filter items by title matching |
| `exclude_pattern` | Regex pattern for item titles to exclude | - | Skip items with matching titles |
| `number_of_threads` | Number of processing threads | `1` | Concurrent list processing |
| `default_permissions` | Default role assignments | - | Additional permissions for all items |

#### SharePoint List Implementation Details

The SharePointListDataStore provides comprehensive crawling and indexing of SharePoint lists and list items with the following capabilities:

**Core Functionality:**
- **List Item Indexing**: Each SharePoint list item becomes a searchable entity with dynamic field extraction and content aggregation
- **Site-Specific Crawling**: Requires a `site_id` parameter to target lists within a specific SharePoint site
- **List Filtering**: Supports crawling all lists or specific lists using `list_id`, with exclusion capabilities via `exclude_list_id`
- **Template-Based Filtering**: Filter lists by SharePoint template types (e.g., 100 for Generic List, 101 for Document Library)
- **System List Exclusion**: Automatically skips system lists unless explicitly configured otherwise

**Content Extraction Strategy:**
The implementation intelligently extracts content from list items:
- **Title Extraction**: Searches for title in common fields (Title, LinkTitle, FileLeafRef)
- **Content Building**: Aggregates text from content fields (Body, Description, Comments, Notes)
- **Dynamic Field Mapping**: Captures all custom SharePoint fields in the `item.fields` map
- **Field Expansion**: Automatically expands field data if not initially available via `$expand=fields`
- **System Field Filtering**: Excludes internal SharePoint system fields from content aggregation

**Multi-Threading Support:**
- Configurable concurrent processing using `number_of_threads` parameter
- Thread pool management with proper resource cleanup
- Parallel processing of multiple lists and list items
- Graceful handling of thread interruption

**Error Handling & Resilience:**
- **Failure Tracking**: Integration with Fess failure URL service for error monitoring
- **Configurable Error Handling**: `ignore_error` parameter controls continuation on failures
- **Statistical Tracking**: Monitors crawling progress with document counts and timing metrics
- **Comprehensive Logging**: Debug and info level logging for troubleshooting

**Permission Management:**
- Extracts SharePoint list and item permissions via Microsoft Graph API
- Maps Microsoft 365 access control to Fess role-based security model
- Supports default permission assignment through configuration
- Inherits site and list-level permissions for items

**Attachment Support:**
- **List Item Attachments**: Detects and processes file attachments on SharePoint list items
- **Attachment Metadata**: Extracts attachment metadata including names, URLs, and file information
- **Content Integration**: Includes attachment information in indexed content for comprehensive search
- **Secure Access**: Inherits SharePoint permissions for proper access control to attached files

**URL Filtering:**
- **Include Pattern**: Regex-based filtering to include specific items by title
- **Exclude Pattern**: Regex-based filtering to exclude items by title
- Efficient pattern matching with pre-compiled regex patterns

**Use Cases:**
- **Structured Data Search**: Index and search custom business data stored in SharePoint lists
- **Task and Issue Tracking**: Search across task lists, issue trackers, and project lists
- **Document Metadata**: Index document libraries managed as SharePoint lists
- **List Attachments**: Search file attachments uploaded to SharePoint list items
- **Custom Applications**: Search data from Power Apps and custom SharePoint solutions
- **Business Process Content**: Index workflow-related lists and approval items

**List Template Types:**
Common SharePoint list template IDs for filtering:
- `100`: Generic List (Custom Lists)
- `101`: Document Library
- `102`: Survey
- `103`: Links
- `104`: Announcements
- `105`: Contacts
- `106`: Events
- `107`: Tasks
- `108`: Discussion Board
- `109`: Picture Library

**Performance Optimizations:**
- Efficient list enumeration with pagination support
- Lazy loading of list items with Microsoft Graph PageIterator
- Memory-efficient processing of large lists
- Caching of compiled regex patterns for filtering

### SharePoint Pages Parameters

| Parameter | Description | Default | Notes |
| --- | --- | --- | --- |
| `site_id` | SharePoint site ID containing pages | All sites | Full site ID format: `hostname,siteCollectionId,siteId` |
| `exclude_site_id` | Comma-separated site IDs to exclude | - | Multiple site IDs separated by commas |
| `ignore_system_pages` | Skip system pages | `true` | Excludes Forms, DevHome, and other system pages |
| `page_type_filter` | Filter by page type | All types | Comma-separated: `news,article,wiki,page` |
| `ignore_error` | Continue crawling on errors | `false` | Set to `true` to skip failed pages |
| `include_pattern` | Regex pattern for page URLs to include | - | Filter pages by URL matching |
| `exclude_pattern` | Regex pattern for page URLs to exclude | - | Skip pages with matching URLs |
| `number_of_threads` | Number of processing threads | `1` | Concurrent page processing |
| `default_permissions` | Default role assignments | - | Additional permissions for all pages |

**Crawling Modes**:
- **All Sites**: Leave `site_id` empty to crawl pages from all accessible sites
- **Specific Site**: Set `site_id` to crawl only pages from that site
- **Filtered Content**: Use `page_type_filter` to limit to specific page types (news, articles, etc.)

**Content Processing**: Pages are processed with canvas layout expansion to extract rich content from web parts, including text formatting and embedded data when available through the Microsoft Graph API.

## 🔧 Development

### Tech Stack

- **Language**: Java 21
- **Build Tool**: Maven 3.8+
- **Framework**: Fess Data Store (LastaFlute/DBFlute)
- **API Client**: Microsoft Graph SDK v6
- **Authentication**: Azure Identity SDK
- **Testing**: UTFlute with JUnit 4
- **Dependency Management**: Maven Shade Plugin with relocation

### Project Structure

```
src/
├── main/java/org/codelibs/fess/ds/ms365/
│   ├── Microsoft365DataStore.java        # Abstract base class
│   ├── OneDriveDataStore.java            # OneDrive implementation
│   ├── OneNoteDataStore.java             # OneNote implementation
│   ├── TeamsDataStore.java               # Teams implementation
│   ├── SharePointDocLibDataStore.java    # SharePoint doc libs
│   ├── SharePointListDataStore.java      # SharePoint lists
│   ├── SharePointPageDataStore.java      # SharePoint pages
│   └── client/
│       └── Microsoft365Client.java       # Graph API wrapper
├── main/resources/
│   └── fess_ds++.xml                     # DI configuration
└── test/java/org/codelibs/fess/ds/ms365/ # Test classes
```

### Building the Project

```bash
# Clean build
mvn clean package

# Run tests
mvn test

# Run specific test
mvn test -Dtest=OneDriveDataStoreTest

# Format code
mvn formatter:format

# Build without tests (faster)
mvn clean package -DskipTests
```

### Development Setup

1. **Prerequisites**:
   ```bash
   # Check Java version
   java -version  # Should be 21+
   
   # Check Maven version  
   mvn -version   # Should be 3.8+
   ```

2. **Clone and Setup**:
   ```bash
   git clone https://github.com/codelibs/fess-ds-microsoft365.git
   cd fess-ds-microsoft365
   
   # Install parent POM
   git clone https://github.com/codelibs/fess-parent.git
   cd fess-parent && mvn install -Dgpg.skip=true && cd ..
   
   # Build project
   mvn clean compile
   ```

3. **IDE Setup**:
   - Import as Maven project
   - Set Java 21 as project SDK
   - Enable annotation processing
   - Use the Eclipse formatter config in `src/config/eclipse/formatter/`

### Testing Strategy

The project uses UTFlute framework with mock Microsoft Graph API responses:

```bash
# Run all tests
mvn test

# Test specific data store
mvn test -Dtest=OneDriveDataStoreTest
mvn test -Dtest=Microsoft365ClientTest

# Test with debug output
mvn test -X -Dtest=SharePointDocLibDataStoreTest
```

### Contributing Guidelines

1. **Code Style**: Use the provided Eclipse formatter configuration
2. **Testing**: Write tests for new functionality using existing patterns
3. **Documentation**: Update README and JavaDocs for API changes
4. **Versioning**: Follow semantic versioning for releases
5. **Pull Requests**: Ensure CI passes before submitting

### Dependency Management

The project uses Maven Shade Plugin to bundle Microsoft Graph SDK dependencies with package relocation to avoid conflicts:

- **Relocated Packages**: `io.netty.*` → `org.codelibs.fess.ds.ms365.netty.*`
- **Bundled Libraries**: Azure SDK, Microsoft Graph SDK v6, Reactor Netty
- **Provided Dependencies**: Fess framework, OpenSearch, Jakarta APIs

## 📋 Usage Examples

### Example 1: OneDrive File Search
```javascript
// Fess search script mapping for OneDrive
title=file.name
content=file.description + "\n" + file.contents  
mimetype=file.mimetype
created=file.created
last_modified=file.last_modified
url=file.web_url
role=file.roles
```

### Example 2: SharePoint List Configuration
```properties
# SharePoint list crawling with filtering
site_id=contoso.sharepoint.com,686d3f1a-a383-4367-b5f5-93b99baabcf3,12048306-4e53-420e-bd7c-31af611f6d8a
list_template_filter=100,101  # Generic lists and Document Libraries
ignore_system_lists=true
include_pattern=.*Important.*
exclude_pattern=.*Draft.*
ignore_error=false
number_of_threads=2
default_permissions={role}sharepoint-users

# Crawl specific list only
# list_id=12345678-1234-1234-1234-123456789abc

# Exclude multiple lists
# exclude_list_id=list1-id,list2-id,list3-id
```

### Example 3: Teams Content Search
```javascript
// Teams message indexing script
title=message.title
content=message.content
created=message.created_date_time
last_modified=message.last_modified_date_time
url=message.web_url
role=message.roles
// Access additional fields
team_name=team.displayName
channel_name=channel.displayName
sender=message.from.user.displayName
```

### Example 4: Teams Configuration
```properties
# Crawl specific team with filters
team_id=12345678-1234-1234-1234-123456789abc
ignore_replies=true
ignore_system_events=true
append_attachment=true
number_of_threads=2
title_dateformat=yyyy/MM/dd'T'HH:mm:ss
title_timezone=Asia/Tokyo

# Exclude multiple teams
exclude_team_ids=team1-id,team2-id,team3-id
include_visibility=public,private

# Crawl specific channel in a team
# channel_id=19:channel-id@thread.tacv2

# Crawl specific chat conversation
# chat_id=19:chat-id@thread.v2
```

### Example 5: SharePoint Pages Content Search
```javascript
// SharePoint pages indexing script
title=page.title
content=page.content
created=page.created
last_modified=page.modified
url=page.url
role=page.roles
// Access additional page fields
page_type=page.type
author=page.author
site_name=page.site_name
description=page.description
```

### Example 6: SharePoint Pages Configuration
```properties
# Crawl pages from all sites with content filtering
ignore_system_pages=true
page_type_filter=news,article
include_pattern=.*important.*|.*announcement.*
exclude_pattern=.*draft.*|.*temp.*
number_of_threads=2
ignore_error=false

# Crawl pages from specific site only
# site_id=contoso.sharepoint.com,686d3f1a-a383-4367-b5f5-93b99baabcf3,12048306-4e53-420e-bd7c-31af611f6d8a

# Exclude multiple sites
# exclude_site_id=site1.sharepoint.com,guid1,guid1;site2.sharepoint.com,guid2,guid2
```

## 🔍 Troubleshooting

### Common Issues

**Authentication Errors**
```
Solution: Verify Azure AD app permissions and admin consent
- Check tenant ID format
- Ensure client secret hasn't expired
- Verify API permissions are granted
```

**Rate Limiting**
```
Solution: Adjust threading and implement backoff
- Reduce number_of_threads parameter
- Enable ignore_error to continue on throttling
- Monitor Microsoft Graph API limits
```

**Large Content Issues**
```
Solution: Configure content handling
- Implement exclude_pattern for large files
- Use OneDriveDataStore settings for file content extraction
```

### Debug Mode

Enable debug logging in Fess to troubleshoot issues:

```xml
<!-- Add to log4j2.xml -->
<Logger name="org.codelibs.fess.ds.ms365" level="DEBUG"/>
```

## 📚 Additional Resources

- **Fess Documentation**: https://fess.codelibs.org/
- **Microsoft Graph API**: https://docs.microsoft.com/en-us/graph/
- **Azure AD App Registration**: https://docs.microsoft.com/en-us/azure/active-directory/develop/
- **Issue Tracker**: https://github.com/codelibs/fess-ds-microsoft365/issues

## 📄 License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

