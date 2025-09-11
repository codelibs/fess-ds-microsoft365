# Microsoft365 Data Store for Fess

[![Java CI with Maven](https://github.com/codelibs/fess-ds-microsoft365/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-microsoft365/actions/workflows/maven.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.codelibs.fess/fess-ds-microsoft365.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.codelibs.fess%22%20AND%20a:%22fess-ds-microsoft365%22)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A powerful Fess Data Store plugin that enables enterprise search across Microsoft 365 services including OneDrive, OneNote, Teams, SharePoint Document Libraries, and SharePoint Lists via Microsoft Graph API v6.

## Overview

This plugin extends [Fess](https://fess.codelibs.org/) enterprise search capabilities to comprehensively index Microsoft 365 content, providing unified search across your organization's cloud documents, conversations, and data with role-based access control integration.

## ✨ Key Features

### 📁 **Comprehensive Content Crawling**
- **OneDrive**: User and group files, folders with metadata extraction
- **OneNote**: Complete notebooks with aggregated content from all sections and pages, supporting site, user, and group notebooks
- **Teams**: Channels, messages, chats with conversation context
- **SharePoint Document Libraries**: Sites and document libraries with enhanced content aggregation
- **SharePoint Lists**: Custom lists and list items with dynamic field mapping

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

The plugin provides five specialized data store types, each optimized for different Microsoft 365 services:

| Data Store | Service | Content Types | Use Cases |
|------------|---------|---------------|----------|
| `oneDriveDataStore` | OneDrive | Files, Folders, Metadata | Document search, file discovery |
| `oneNoteDataStore` | OneNote | Notebooks (with sections & pages content) | Knowledge base search, note finding, documentation search |
| `teamsDataStore` | Teams | Channels, Messages, Chats | Conversation search, team communication |
| `sharePointDocLibDataStore` | SharePoint | Document Libraries, Files | Document management, content discovery |
| `sharePointListDataStore` | SharePoint | Lists, List Items, Custom Fields | Structured data search, business process content |

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
| doclib.url | The Microsoft Graph API web URL for the document library. |
| doclib.canonical_url | The standardized SharePoint URL for accessing the document library. |
| doclib.created | The time at which the document library was created. |
| doclib.modified | The last time the document library was modified. |
| doclib.type | The type of the drive (e.g., "documentLibrary"). |
| doclib.site_name | The display name of the SharePoint site containing this document library. |
| doclib.site_url | The web URL of the SharePoint site. |
| doclib.roles | Users/groups who can access the document library. |

**Note**: SharePointDocLibDataStore indexes document libraries as individual searchable entities, combining library metadata with site information to provide comprehensive search content. The `doclib.content` field aggregates the library name, description, and parent site name for enhanced discoverability.

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
| `include_visibility` | Team visibility levels to include | All | `public`, `private` |
| `channel_id` | Specific channel ID to crawl | All channels | Within specified team |
| `chat_id` | Specific chat ID to crawl | - | For 1:1 or group chats |
| `ignore_replies` | Skip reply messages | `false` | Process only root messages |
| `append_attachment` | Include attachments in content | `true` | Append attachment text to message body |
| `ignore_system_events` | Skip system event messages | `true` | Filter out system notifications |
| `title_dateformat` | Date format for message titles | `yyyy/MM/dd'T'HH:mm:ss` | Java date pattern |
| `title_timezone_offset` | Timezone offset for titles | `Z` | e.g., `+09:00`, `-05:00` |

**Crawling Modes**:
- **All Teams**: Leave `team_id` empty to crawl all accessible teams
- **Specific Team**: Set `team_id` to crawl only that team's channels
- **Specific Channel**: Set both `team_id` and `channel_id`
- **Chat Messages**: Set `chat_id` to crawl a specific chat (messages are aggregated)

### OneNote-Specific Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `site_note_crawler` | Enable crawling of site notebooks | `true` | Crawls notebooks at the root SharePoint site |
| `user_note_crawler` | Enable crawling of user notebooks | `true` | Crawls personal OneNote notebooks for licensed users |
| `group_note_crawler` | Enable crawling of group notebooks | `true` | Crawls shared notebooks in Microsoft 365 groups |
| `number_of_threads` | Number of processing threads | `1` | Controls concurrent notebook processing |

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

### SharePoint Document Library Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `site_id` | Specific site ID to crawl | All sites | Can be site URL or GUID |
| `exclude_site_id` | Site IDs to exclude | - | See format guide below |
| `site_type_filter` | Filter by type | - | `root`, `subsite` |
| `ignore_system_libraries` | Skip system libraries | `true` | Excludes Form Templates, etc. |
| `ignore_folder` | Skip folder documents | `true` | Index folder structure |

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

**Recent Improvements**: SharePoint List crawling now includes enhanced statistical tracking, improved error handling with configurable failure recording, comprehensive URL filtering support, and robust permission processing to ensure secure and efficient list item indexing.

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

# Exclude multiple teams
exclude_team_ids=team1-id,team2-id,team3-id
include_visibility=public
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

