package utils

import com.evidence.api.thrift.v1.TidEntities
import com.evidence.service.common.logging.LazyLogging
import play.api.libs.json._
import utils.jsonapi.{AnyUriSegment, TidValueUriSegment, TypeValueUriSegment, UriSegment}

import scala.language.implicitConversions

trait JsonApiOrg extends LazyLogging {
  implicit def tupleStrToUriSegment(value: (String, String)): UriSegment = TypeValueUriSegment(value._1, value._2)

  implicit def tupleTidToUriSegment(value: (TidEntities, String)): UriSegment = TidValueUriSegment(value._1, value._2)

  implicit def stringToUriSegment(value: String): UriSegment = AnyUriSegment(value)

  protected final val urlBase = "/api/v1"

  // JSONAPI.org
  protected final val JSONAPIContentType = "application/vnd.api+json"
  protected final val metaField = "meta"
  protected final val dataField = "data"
  protected final val errorsField = "errors"
  protected final val serviceErrorField = "serviceError"
  protected final val linksField = "links"
  protected final val selfField = "self"
  protected final val relatedField = "related"
  protected final val includedField = "included"
  protected final val attributesField = "attributes"
  protected final val relationshipsField = "relationships"
  protected final val typeField = "type"
  protected final val sharerIdField = "sharerId"
  protected final val idField = "id"
  protected final val shouldCheckPermissionAsyncField = "shouldCheckPermissionAsync"
  protected final val contentField = "content"

  // Custom Identifier
  protected final val canonicalStringField = "errorInfo"
  protected final val domainField = "domain" // Many of our resources are defined by (type, id, domain) instead of (type, id)
  protected final val resourceField = "resource"

  // Pagination
  protected final val offsetField = "offset"
  protected final val pageField = "page"
  protected final val pagesField = "pages"
  protected final val limitField = "limit"
  protected final val countField = "count"
  protected final val prevField = "prev"
  protected final val nextField = "next"
  protected final val firstField = "first"
  protected final val lastField = "last"
  protected final val sizeField = "size"
  protected final val totalHitsField = "totalHits"
  protected final val paginationTypeField = "paginationType"
  protected final val totalField = "total"

  // Faceting
  protected final val facetFields = "facetFields"

  // Common nouns
  protected final val nameField = "name"
  protected final val externalIdField = "externalId"
  protected final val statusField = "status"
  protected final val status2Field = "status2"
  protected final val errorStatusField = "errorStatus"
  protected final val codeField = "code"
  protected final val entitiesField = "entities"
  protected final val failedIdsField = "failedIds"
  protected final val failedEntitiesField = "failedEntities"
  protected final val titleField = "title"
  protected final val deploymentField = "deployment"
  protected final val descriptionField = "description"
  protected final val detailField = "detail"
  protected final val sourceField = "source"
  protected final val createdOnField = "createdOn"
  protected final val modifiedOnField = "modifiedOn"
  protected final val lastModifiedField = "lastModified"
  protected final val lastModifiedByTidField = "lastModifiedByTid"
  protected final val createdField = "created"
  protected final val creatorField = "creator"
  protected final val evidenceField = "evidence"
  protected final val successField = "success"
  protected final val valueField = "value"
  protected final val valuesField = "values"
  protected final val versionField = "version"
  protected final val urlField = "url"
  protected final val clientIdField = "clientId"
  protected final val clientSecretField = "clientSecret"
  protected final val errorField = "error"
  protected final val errorDataField = "errorData"
  protected final val messageField = "message"
  protected final val availabilityField = "availability"
  protected final val maintenanceField = "maintenance"
  protected final val snfHealthEntryResponseField = "snfHealthEntryResponse"
  protected final val shareAccessField = "shareAccess"
  protected final val isCoachingField = "isCoaching"
  protected final val isInHierarchyField = "isInHierarchy"
  protected final val syncedOnField = "syncedOn"
  protected final val monitorsField = "monitors"
  protected final val membersField = "members"
  protected final val updatedOnField = "updatedOn"
  protected final val userStatusField = "status"
  protected final val substatusField = "substatus"
  protected final val purposeField = "purpose"
  protected final val commentField = "comment"
  protected final val recipientField = "recipient"
  protected final val dateTimeField = "dateTime"
  protected final val regionsField = "regions"
  protected final val serverTimestampField = "serverTimestamp"
  protected final val folderPathField = "folderPath"
  protected final val parentField = "parent"
  protected final val caseField = "case"
  protected final val shareLogReportsField = "shareLogReports"
  protected final val formatField = "format"
  protected final val shareLogCountField = "shareLogCount"
  protected final val blobIdField = "blobId"
  protected final val dataSchemaVersionField = "dataSchemaVersion"

  // Types - SINGULAR STANDARD
  protected final val agencyType = "agency"
  protected final val assetString = "asset"
  protected final val groupType = "group"
  protected final val userType = "user"
  protected final val emailType = "email"
  protected final val permalinkType = "permalink"
  protected final val unknownType = "unknown"
  protected final val workerType = "worker"
  protected final val clientType = "client"
  protected final val deviceType = "device"
  protected final val deviceLogType = "deviceLog"
  protected final val caseType = "case"
  protected final val folderType = "folder"
  protected final val folderPathType = "folderPath"
  protected final val evidenceType = "evidence"
  protected final val pinnedEvidenceType = "pinnedEvidence"
  protected final val accessListType = "accessList"
  protected final val liveStreamType = "liveStream"
  protected final val tagType = "tag"
  protected final val categoryType = "category"
  protected final val featureType = "feature"
  protected final val pointType = "point"
  protected final val geometryType = "geometry"
  protected final val coordinatesType = "coordinates"
  protected final val roleType = "role"
  protected final val fileType = "file"
  protected final val clipType = "clip"
  protected final val actorsType = "actors"
  protected final val sessionType = "session"
  protected final val transcoderSvc = "transcoderService"
  protected final val eventsHandlerSvc = "eventsHandlerService"
  protected final val locationField = "location"
  protected final val schemasField = "schemas" // should be schema, don't copy
  protected final val vehicleType = "vehicle"
  protected final val networkConfigType = "networkConfig"
  protected final val fleetCameraConfigType = "fleetCameraConfig"
  protected final val ingestConfig = "ingestConfig"
  protected final val authTokenType = "authToken"
  protected final val noteType = "note"
  protected final val annotationType = "annotation"
  protected final val signalConfigType = "signalConfig"
  protected final val snfServerType = "snfServer"
  protected final val pesPortalType = "pesPortal"
  protected final val pesLinkType = "pesLink"
  protected final val submissionType = "submission"
  protected final val uploadType = "upload"
  protected final val restrictionLevel = "restriction_level"
  protected final val members = "members"
  protected final val shareType = "share"
  protected final val sharesType = "shares"
  protected final val serviceAccountType = "serviceaccount"
  protected final val caseMembers = "caseMembers"
  protected final val functionalTestStats = "functionalTestStats"
  protected final val formTemplateType = "eformsTemplate"
  protected final val shareLogReportsType = "shareLogReport"
  protected final val shareLogDataType = "shareLogData"
  protected final val shareLogReportType = "shareLogReport"

  // Users - Meta Information
  protected final val userCountField = "userCount"
  protected final val userMessageField = "userMessage"

  // API Common Keys
  protected final val additionalIncludesField = "additionalIncludes"
  protected final val createdByField = "createdBy"
  protected final val ephemeralRequestIdField = "ephemeralRequestId"
  protected final val expiryField = "expiry"
  protected final val downloadsField = "downloads"
  protected final val createdDateField = "createdDate"
  protected final val shareLogType = "shareLog"
  protected final val shareLogTypeField = "shareType"
  protected final val uniqueShareField = "uniqueShare"
  protected final val sharedFromField = "sharedFrom"
  protected final val sharedToField = "sharedTo"
  protected final val dateTimeForNameField = "dateTimeForName"

  // This avoids using an Implicit Conversion and Case Classes because it ended up being
  // buggy in various ways. This is probably cleaner and makes more sense for this use case.
  protected def entityUrl(segments: Seq[UriSegment], query: Option[String] = None, fragment: Option[String] = None, urlBase: String = urlBase): String = {
    val builder = new StringBuilder(urlBase)

    segments.foreach {
      case s: TypeValueUriSegment =>
        builder.append("/")
        builder.append(entityToUrlString(s.entityType))
        builder.append('/')
        builder.append(s.value)

      case s: TidValueUriSegment =>
        JsonApiOrgToThriftV1.entityTypeToJson(s.entityType).map { et =>
          val result = entityToUrlString(et.toLowerCase)
          if (result.isEmpty) {
            // if entity type is not supported, then don't display the url in the json response
            builder.clear()
          } else {
            builder.append("/")
            builder.append(result)
            builder.append('/')
            builder.append(s.value)
          }
        }.orElse(throw new Exception(s"TidEntity of ${s.entityType} not supported"))

      case s: AnyUriSegment =>
        builder.append("/")
        builder.append(s.value)
    }

    new java.net.URI(null, null, null, -1, builder.toString(), query.orNull, fragment.orNull).toString
  }

  protected def entityUrl(segments: UriSegment*): String = {
    entityUrl(segments, None, None)
  }

  protected def entityUrl(urlBaseV3: Option[String], segment: UriSegment*): String = {
    if(urlBase.isEmpty) entityUrl(segment, None, None)
    else entityUrl(segment, None, None, urlBaseV3.getOrElse(urlBase))
  }

  protected def entityToUrlString(entityType: String): String = {
    entityType match {
      case `groupType` => "groups"
      case `userType` => "users"
      case `agencyType` => "agencies"
      case `clientType` => "clients"
      case "token" => "clients"
      case `deviceType` => "devices"
      case `caseType` => "cases"
      case `evidenceType` => "evidence"
      case `roleType` => "roles"
      case `fileType` => "files"
      case `vehicleType` => "vehicles"
      case `pesPortalType` => "pes/portals"
      case `folderType` => "folders"
      case _ =>
        logger.info("unsupportedEntityType")("entityType" -> entityType)
        ""
    }
  }

  /**
    * Removes any null-value'd keys from a JsObject.  Recursive.
    * @return json with all null-or-empty values removed.
    */
  protected def removeNullValues(incomingJson: JsObject): JsObject = {
    /**
      * Given a JsValue, can it be said that it is truly a value?
      */
    def withValue(v: JsValue) = v match {
      case JsNull => false
      case JsString("") => false
      case _ => true
    }

    def iterateArray: PartialFunction[JsValue, JsValue] = {
      case z: JsObject => JsObject(z.fields.collect(recurse)) // Json maps get the trim-treatment...
      case z: JsValue if withValue(z) => z // ...individual values are checked.
    }

    def recurse: PartialFunction[(String, JsValue), (String, JsValue)] = {
      // Arrays can hold many a null-containing treasure...
      case (x: String, y: JsArray) => (x, JsArray(y.value.collect(iterateArray)))
      // Recurse through the sub-map's fields so it can get a haircut too.
      case (x: String, y: JsObject) => (x, JsObject(y.fields.collect(recurse)))
      // Key-value pairs are kept if there's a value.
      case (x: String, y: JsValue) if withValue(y) => (x, y)
      // Key-value pairs without a value drop through this partial function.
    }

    JsObject(incomingJson.fields.collect(recurse))
  }
}

object JsonApiOrgToThriftV1 extends JsonApiOrg {

  def entityToUrlString(tidEntities: TidEntities): Option[String] = {
    entityToUrlStringMap.get(tidEntities).orElse {
      logger.info("unsupportedEntityType")("entityType" -> tidEntities.name)
      None
    }
  }

  def urlStringToEntity(value: String): Option[TidEntities] = {
    entityUrlToEntityMap.get(value).orElse {
      logger.info("unsupportedEntityType")("entityType" -> value)
      None
    }
  }

  def entityTypeToJson(tidEntities: TidEntities): Option[String] = {
    entityToJsonMap.get(tidEntities).orElse {
      logger.info("unsupportedEntityType")("entityType" -> tidEntities.name)
      None
    }
  }

  def jsonToEntityType(entity: String): Option[TidEntities] = {
    jsonToEntityMap.get(entity).orElse {
      logger.info("unsupportedEntityType")("entityType" -> entity)
      None
    }
  }

  /**
   * JSON reader for `TidEntities`, but it only supports the limited set of alternative names in `jsonToEntityMap`.
   */
  val limitedEntityTypeReads: Reads[TidEntities] = Reads {
    case JsString(typeString) =>
      jsonToEntityMap.get(typeString) match {
        case Some(entityType) => JsSuccess(entityType)
        case None => JsError(s"unsupportedEntityType entityType=$typeString")
      }
    case _ => JsError("expected.string")
  }

  // evidence is the plural override.
  private final val entityUrlStringMappings: Seq[(String, TidEntities)] = Seq(
    ("groups", TidEntities.Team),
    ("users", TidEntities.Subscriber),
    ("agencies", TidEntities.Partner),
    ("clients", TidEntities.AuthClient),
    ("devices", TidEntities.Device),
    ("cases", TidEntities.Case),
    ("evidence", TidEntities.Evidence),
    ("roles", TidEntities.Role),
    ("files", TidEntities.File),
    ("vehicles", TidEntities.Vehicle),
    ("email", TidEntities.Email),
    ("pes/portals", TidEntities.PesPortal)
  )

  // Avoid duplicates here - make sure the TidEntity is unique
  private final val entityUrlStringMappingsCompat: Seq[(String, TidEntities)] = Seq(
    ("clients", TidEntities.AuthToken))

  private final val entityUrlToEntityMap: Map[String, TidEntities] = entityUrlStringMappings.toMap

  private final val entityToUrlStringMap: Map[TidEntities, String] =
    (entityUrlStringMappings ++ entityUrlStringMappingsCompat).map(e => e._2 -> e._1).toMap

  private final val entityMappings: Seq[(String, TidEntities)] = Seq(
    (agencyType, TidEntities.Partner),
    (userType, TidEntities.Subscriber),
    (groupType, TidEntities.Team),
    (deviceType, TidEntities.Device),
    (clientType, TidEntities.AuthClient),
    (evidenceType, TidEntities.Evidence),
    (caseType, TidEntities.Case),
    (roleType, TidEntities.Role),
    (fileType, TidEntities.File),
    (vehicleType, TidEntities.Vehicle),
    (transcoderSvc, TidEntities.TcoderSvc),
    (authTokenType, TidEntities.AuthToken),
    (eventsHandlerSvc, TidEntities.EvhSvc),
    (emailType, TidEntities.Email),
    (permalinkType, TidEntities.Permalink),
    (unknownType, TidEntities.Unknown),
    (workerType, TidEntities.Worker),
    (pesPortalType, TidEntities.PesPortal),
    (serviceAccountType, TidEntities.ServiceAccount),
    (folderType, TidEntities.Folder)
  )

  private final val entityMappingsCompat: Seq[(String, TidEntities)] = Seq(
    ("token", TidEntities.AuthClient))

  private final val jsonToEntityMap: Map[String, TidEntities] = (entityMappings ++ entityMappingsCompat).toMap

  private final val entityToJsonMap: Map[TidEntities, String] = entityMappings.map(e => e._2 -> e._1).toMap
}
