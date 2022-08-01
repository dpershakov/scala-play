package services.arsenal

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

import com.evidence.api.thrift.v1.TidEntities
import com.evidence.service.arsenal.thrift._
import com.evidence.service.common.ExecutionContexts.blockingIoContext
import com.evidence.service.common.{Convert, StringUtils, Tid => CommonTid}
import controllers.devices.DeviceConstants
import play.api.http.Status
import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._
import play.api.mvc.{Result, Results}
import services.arsenal.objects.firmwareInfo.{GetAgencyFirmwareVersionsResponse, JsonRootAgencyFirmwareInfo}
import services.komrade.TeamDetails
import services.solr.{SolrConstants, SolrResponse}
import utils.jsonapi.JsonWriteHelpers
import utils.permissions.JWTWrapper
import utils.{JsonApiOrg, JsonApiOrgToThriftV1, TwitterFutureRescueHelper}

import scala.concurrent.Future

trait ArsenalConversions extends JsonApiOrg with TwitterFutureRescueHelper[ServiceErrorCode] with Results with Status {

  protected val bulkUpdateLimitExceededError: Result = {
    errorResponse(
      resultStatus = Results.BadRequest,
      title = Some(BulkUpdateLimitExceededString),
      detail = Some(BulkUpdateLimitExceededString),
      serviceErrorCode = Some(ServiceErrorCode.LimitExceeded.value),
      canonicalString = Some(BulkUpdateLimitExceededCanonicalString)
    )
  }

  protected final val BulkUpdateLimitExceededCanonicalString = "bulk_update_limit_exceeded"
  protected final val BulkUpdateLimitExceededString = "Bulk Update Limit Exceeded"
  protected final val DeviceBelongsToAnotherAgencyCanonicalString = "device_belongs_to_another_agency"
  protected final val DeviceBelongsToAnotherAgencyString = "Device belongs to another agency"
  protected final val DuplicateString = "Duplicate"
  protected final val InternalServerErrorCanonicalString = "internal_server_error"
  protected final val InternalServerErrorString = "Internal Server Error"
  protected final val InvalidDateCanonicalString = "invalid_date"
  protected final val InvalidDateString = "Invalid Date"
  protected final val InvalidInputCanonicalString = "invalid_input"
  protected final val InvalidInputString = "Invalid Input"
  protected final val InvalidModelCanonicalString = "invalid_model"
  protected final val InvalidModelString = "Invalid Model"
  protected final val InvalidOwnerCanonicalString = "invalid_owner"
  protected final val InvalidOwnerString = "Invalid Owner"
  protected final val InvalidSerialCanonicalString = "invalid_serial"
  protected final val InvalidSerialString = "Invalid Serial"
  protected final val InvalidStatusCanonicalString = "invalid_status"
  protected final val InvalidStatusString = "Invalid Status"
  protected final val InvalidStatusTransitionCanonicalString = "invalid_status_transition"
  protected final val InvalidStatusTransitionString = "Invalid Status Transition"
  protected final val OkString = "Ok"
  protected final val OperationNotPermittedCanonicalString = "operation_not_permitted"
  protected final val OperationNotPermittedString = "Operation Not Permitted"
  protected final val TooManyRequestsCanonicalString = "too_many_requests"
  protected final val TooManyRequestsString = "Too Many Requests"
  protected final val ResourceNotFoundCanonicalString = "resource_not_found"
  protected final val ResourceNotFoundString = "Resource Not Found"
  protected final val UnauthorizedString = "Unauthorized"
  protected final val deviceHomeIdField = "homeId"
  protected final val lteInfoField = "lteInfo"
  protected final val assignedByField = "assignedBy"
  protected final val assignedCarrierField = "assignedCarrier"

  protected final val assetHomeField = "assetHome"
  protected final val modelField = "model"
  protected final val agencyIdField = "agencyId"
  protected final val assetTypeField = "assetType"
  protected final val assetTypeIdField = "assetTypeId"
  protected final val deviceIdField = "deviceId"
  protected final val updatedByField = "updatedBy"
  protected final val updateReasonField = "updateReason"
  protected final val preferredLteCarrierField = "preferredLteCarrier"

  protected lazy final val DuplicateCanonicalString = DuplicateString.toLowerCase
  protected lazy final val OkCanonicalString = OkString.toLowerCase
  protected lazy final val UnauthorizedCanonicalString = UnauthorizedString.toLowerCase

  private final val activationsField = "activations"
  private final val activeUsersField = "activeUsers"
  private final val allowUploads = "allowUploads"
  private final val configField = "config"
  private final val dateAssignedField = "dateAssigned"
  private final val dateCreatedField = "dateCreated"
  private final val dateModifiedField = "dateModified"
  private final val deviceCanaryField = "canary"
  private final val deviceErrorsField = "deviceErrors"

  private final val actionField = "action"
  private final val artifactTypeField = "artifactType"
  private final val deviceLogFriendlyNameField = "friendlyName"
  private final val deviceLogKeyField = "fileKey"
  private final val deviceLogUploadedAt = "uploadedAt"
  private final val deviceLogUrlField = "url"
  private final val deviceLogVersionField = "version"
  private final val deviceModelField = "deviceModel"
  private final val deviceSerialField = "serial"
  private final val devicesField = "devices"
  private final val expirationDateField = "expirationDate"
  private final val firmwareDownloadUrlField = "downloadUrl"
  private final val firmwareVersionField = "firmwareVersion"
  private final val fleetCameraConfigsField = "fleetCameraConfigs"
  private final val friendlyNameField = "friendlyName"
  private final val gpiosField = "gpios"
  private final val highToLowField = "highToLow"
  private final val iccid1Field = "iccid1"
  private final val iccid2Field = "iccid2"
  private final val imeiField = "imei"
  private final val keyField = "key"
  private final val lastIssuedToTidField = "lastIssuedToTid"
  private final val lastUpdatedTimestampField = "lastUpdatedTimestamp"
  private final val lastUploadDateTimeField = "lastUploadDateTime"
  private final val makeField = "make"
  private final val modemModelField = "modemModel"
  private final val networkConfigTypeField = "networkConfigType"
  private final val networkConfigsField = "networkConfigs"
  private final val numberField = "number"
  private final val ownerField = "owner"
  private final val ownerEvidenceGroupField = "ownerEvidenceGroup"
  private final val passphrase = "passphrase"
  private final val preferences = "preferences"
  private final val primaryHubIdField = "primaryHubId"
  private final val primaryHubSerialField = "primaryHubSerial"
  private final val priority = "priority"
  private final val propsField = "props"
  private final val security = "security"
  private final val severityField = "severity"
  private final val smartRouter = "smartRouter"
  private final val ssid = "ssid"
  private final val startDateField = "startDate"
  private final val subTypeField = "subType"
  private final val supportedLteCarriersField = "supportedLteCarriers"
  private final val overriddenLteCarriersField = "overriddenLteCarriers"
  private final val timestampField = "timestamp"
  private final val typeFieldDeprecated = "type"
  private final val updatedField = "updated"
  private final val vehicleCategoryField = "category"
  private final val vehicleGenerationField = "vehicleGeneration"
  private final val vehicleHomeIdField = "vehicleHomeId"
  private final val vehicleHomeNameField = "vehicleHomeName"
  private final val vehicleNameField = "name"
  private final val warrantiesField = "warranties"
  private final val warrantyExpiryDateTimeField = "warrantyExpiry"
  private final val wirelessOffloadField = "wirelessOffload"

  private val arsenalErrorCodeToHttpStatusMetadataMap: Map[ServiceErrorCode, HttpStatusMetadata] = Map(
    ServiceErrorCode.AuthenticationFailed -> HttpStatusMetadata(
      resultStatus = Results.Unauthorized,
      httpStatus = UNAUTHORIZED,
      title = Some(UnauthorizedString),
      detail = Some(UnauthorizedString),
      serviceErrorCode = Some(ServiceErrorCode.AuthenticationFailed.value),
      canonicalString = UnauthorizedCanonicalString
    ),
    ServiceErrorCode.LimitExceeded -> HttpStatusMetadata(
      resultStatus = Results.BadRequest,
      httpStatus = BAD_REQUEST,
      title = Some(BulkUpdateLimitExceededString),
      detail = Some(BulkUpdateLimitExceededString),
      serviceErrorCode = Some(ServiceErrorCode.LimitExceeded.value),
      canonicalString = BulkUpdateLimitExceededCanonicalString
    ),
    ServiceErrorCode.DeviceBelongsToAnotherAgency -> HttpStatusMetadata(
      resultStatus = Results.Forbidden,
      httpStatus = FORBIDDEN,
      title = Some(DeviceBelongsToAnotherAgencyString),
      detail = Some(DeviceBelongsToAnotherAgencyString),
      serviceErrorCode = Some(ServiceErrorCode.DeviceBelongsToAnotherAgency.value),
      canonicalString = DeviceBelongsToAnotherAgencyCanonicalString
    ),
    ServiceErrorCode.Duplicate -> HttpStatusMetadata(
      resultStatus = Results.Conflict,
      httpStatus = CONFLICT,
      title = Some(DuplicateString),
      detail = Some(DuplicateString),
      serviceErrorCode = Some(ServiceErrorCode.Duplicate.value),
      canonicalString = DuplicateCanonicalString
    ),
    ServiceErrorCode.Exception -> HttpStatusMetadata(
      resultStatus = Results.InternalServerError,
      httpStatus = INTERNAL_SERVER_ERROR,
      title = Some(InternalServerErrorString),
      detail = Some(InternalServerErrorString),
      serviceErrorCode = Some(ServiceErrorCode.Exception.value),
      canonicalString = InternalServerErrorCanonicalString
    ),
    ServiceErrorCode.InvalidDate -> HttpStatusMetadata(
      resultStatus = Results.BadRequest,
      httpStatus = BAD_REQUEST,
      title = Some(InvalidDateString),
      detail = Some(InvalidDateString),
      serviceErrorCode = Some(ServiceErrorCode.InvalidDate.value),
      canonicalString = InvalidDateCanonicalString
    ),
    ServiceErrorCode.InvalidInput -> HttpStatusMetadata(
      resultStatus = Results.BadRequest,
      httpStatus = BAD_REQUEST,
      title = Some(InvalidInputString),
      detail = Some(InvalidInputString),
      serviceErrorCode = Some(ServiceErrorCode.InvalidInput.value),
      canonicalString = InvalidInputCanonicalString
    ),
    ServiceErrorCode.InvalidModel -> HttpStatusMetadata(
      resultStatus = Results.BadRequest,
      httpStatus = BAD_REQUEST,
      title = Some(InvalidModelString),
      detail = Some(InvalidModelString),
      serviceErrorCode = Some(ServiceErrorCode.InvalidModel.value),
      canonicalString = InvalidModelCanonicalString
    ),
    ServiceErrorCode.InvalidSerial -> HttpStatusMetadata(
      resultStatus = Results.BadRequest,
      httpStatus = BAD_REQUEST,
      title = Some(InvalidSerialString),
      detail = Some(InvalidSerialString),
      serviceErrorCode = Some(ServiceErrorCode.InvalidSerial.value),
      canonicalString = InvalidSerialCanonicalString
    ),
    ServiceErrorCode.InvalidStatus -> HttpStatusMetadata(
      resultStatus = Results.BadRequest,
      httpStatus = BAD_REQUEST,
      title = Some(InvalidStatusString),
      detail = Some(InvalidStatusString),
      serviceErrorCode = Some(ServiceErrorCode.InvalidStatus.value),
      canonicalString = InvalidStatusCanonicalString
    ),
    ServiceErrorCode.InvalidStatusTransition -> HttpStatusMetadata(
      resultStatus = Results.InternalServerError,
      httpStatus = INTERNAL_SERVER_ERROR,
      title = Some(InvalidStatusTransitionString),
      detail = Some(InvalidStatusTransitionString),
      serviceErrorCode = Some(ServiceErrorCode.InvalidStatusTransition.value),
      canonicalString = InvalidStatusTransitionCanonicalString
    ),
    ServiceErrorCode.NotFound -> HttpStatusMetadata(
      resultStatus = Results.NotFound,
      httpStatus = NOT_FOUND,
      title = Some(ResourceNotFoundString),
      detail = Some(ResourceNotFoundString),
      serviceErrorCode = Some(ServiceErrorCode.NotFound.value),
      canonicalString = ResourceNotFoundCanonicalString
    ),
    ServiceErrorCode.OwnerInvalid -> HttpStatusMetadata(
      resultStatus = Results.BadRequest,
      httpStatus = BAD_REQUEST,
      title = Some(InvalidOwnerString),
      detail = Some(InvalidOwnerString),
      serviceErrorCode = Some(ServiceErrorCode.OwnerInvalid.value),
      canonicalString = InvalidOwnerCanonicalString
    ),
    ServiceErrorCode.Ok -> HttpStatusMetadata(
      resultStatus = Results.Ok,
      httpStatus = OK,
      serviceErrorCode = Some(ServiceErrorCode.Ok.value),
      canonicalString = OkCanonicalString
    )
  )
  private val translator: RescueTranslator = {
    case se: ServiceException =>
      description: String =>
        logger.warn("arsenalServiceException")("description" -> description, "errorCode" -> se.errorCode)
        se.errorCode
  }

  implicit val teamDetailsNameOnlyWrites = new Writes[TeamDetails] {
    def writes(v: TeamDetails) = toJson(v)

    private def toJson(v: TeamDetails): JsObject = {
      Json.obj(
        typeField -> groupType,
        idField -> v.team.id,
        attributesField -> Json.obj(
          nameField -> v.meta.name, // Ok to show anyone the group names
          agencyIdField -> v.team.domain
        )
      )
    }
  }

  implicit val entityDescWrites = new Writes[EntityDescriptor] {
    def writes(v: EntityDescriptor) = toJson(v)

    def toJson(v: EntityDescriptor): JsObject = {
      Json.obj(
        typeField -> JsonApiOrgToThriftV1.entityTypeToJson(v.entityType),
        idField -> v.id,
        relationshipsField -> Json.obj(
          agencyType -> Json.obj(
            dataField -> Json.obj(
              typeField -> agencyType,
              idField -> v.partnerId
            )
          )
        )
      )
    }
  }

  protected def agencyFirmwareInfoToJson(
    partnerId: UUID,
    model: String,
    models: Set[String],
    baseModel: String,
    agencyFirmwareVersionsResponse: GetAgencyFirmwareVersionsResponse,
    currentFirmwareVersionResponse: GetLatestFirmwareVersionResponse
  ): JsValue = {
    val jsonRootAgencyFirmwareInfo = JsonRootAgencyFirmwareInfo(
      partnerId = partnerId,
      model = model,
      models = models,
      baseModel = baseModel,
      agencyFirmwareVersionsResponse = agencyFirmwareVersionsResponse,
      currentFirmwareVersionResponse = currentFirmwareVersionResponse
    )

    val json = Json.toJson(jsonRootAgencyFirmwareInfo)

    json
  }

  protected def assetInfoToJson(asset: AssetMetaData, assetHomeJson: Option[JsValue]): JsObject = {
    val assetInfo = Json.obj(
      typeField -> assetString,
      idField -> asset.id,
      attributesField -> Json.obj(
        agencyIdField -> asset.partnerId,
        assetTypeField -> Json.obj (
          idField -> asset.assetType.map(_.id),
          nameField -> asset.assetType.map(_.name)
        ),
        deviceSerialField -> asset.serial,
        statusField -> asset.`status`.map(_.toString.toLowerCase),
        friendlyNameField -> asset.friendlyName,
        assetHomeField -> assetHomeJson,
        dateCreatedField -> asset.dateCreated,
        dateAssignedField -> asset.dateAssigned
      ),
      relationshipsField -> Json.obj(
        ownerField -> asset.owner.map(entityDescriptorToRelationshipJson),
        updatedByField -> asset.updatedBy.map(entityDescriptorToRelationshipJson),
        agencyType -> asset.partnerId.map(agencyIdToRelationshipJson)
      )
    )
    createJsonDataObject(assetInfo)
  }

  protected def deviceFirmwareToJson(agencyUuid: UUID, deviceUuid: UUID, firmware: Firmware): JsObject = {
    Json.obj(dataField -> Json.obj(
      deviceIdField -> deviceUuid.toString,
      agencyIdField -> agencyUuid.toString,
      firmwareVersionField -> firmware.displayVersion
    ))
  }

  protected def firmwareToJson(deviceModel: String, artifactType: String, subType: String, firmware: Firmware): JsObject = {
    Json.obj(dataField -> Json.obj(
      deviceModelField -> deviceModel,
      artifactTypeField -> artifactType,
      subTypeField -> subType,
      firmwareVersionField -> firmware.displayVersion
    ))
  }

  protected def firmwareDownloadToJson(deviceModel: String, artifactType: String, subType: String, version: String, downloadUrl: String): JsObject = {
    Json.obj(dataField -> Json.obj(
      deviceModelField -> deviceModel,
      artifactTypeField -> artifactType,
      subTypeField -> subType,
      firmwareVersionField -> version,
      firmwareDownloadUrlField -> downloadUrl
    ))
  }

  protected def deviceFunctionalTestStatsToJson(
    model: String,
    models: Set[String],
    baseModel: String,
    numberOfDays: Int,
    results: Seq[(String, Option[Int])]
  ): JsObject = {
    // convert the data to Json object
    val resultsConverted: Seq[(String, JsValueWrapper)] = results.map{
      case (k,v) => (k, Json.toJsFieldJsValueWrapper(v))
    }

    Json.obj(
      dataField -> Json.obj(
        typeField -> functionalTestStats,
        DeviceConstants.ModelString -> model,
        "models" -> models,
        "baseModel" -> baseModel,
        "functionalTestInfo" -> Json.obj(
          resultsConverted :+ ("numberOfDays",  Json.toJsFieldJsValueWrapper(numberOfDays.toString)) : _*
        )
      )
    )
  }

  protected def buildBulkAssignErrors(secMap: Map[String, ServiceErrorCode]): JsObject = {
    val resultsConverted = secMap.map(x => x._1 -> serviceErrorToJson(arsenalErrorToHttpStatusMetadata(x._2).httpStatus,
                                                               arsenalErrorToHttpStatusMetadata(x._2).title.orNull,
                                                               arsenalErrorToHttpStatusMetadata(x._2).detail.orNull,
                                                               arsenalErrorToHttpStatusMetadata(x._2).canonicalString,
                                                               arsenalErrorToHttpStatusMetadata(x._2).serviceErrorCode))
    Json.obj(errorsField -> Json.toJsFieldJsValueWrapper(resultsConverted))
  }

  protected def arsenalErrorToResponse(sec: ServiceErrorCode): Result = {
    val httpStatusMetadata = arsenalErrorToHttpStatusMetadata(sec)
    errorResponse(
      resultStatus = httpStatusMetadata.resultStatus,
      title = httpStatusMetadata.title,
      detail = httpStatusMetadata.detail,
      canonicalString = Some(httpStatusMetadata.canonicalString),
      serviceErrorCode = httpStatusMetadata.serviceErrorCode
    )
  }

  protected def createJsonDataObject(json: JsValue): JsObject = {
    Json.obj(dataField -> json)
  }

  protected def deviceActivationsToJson(deviceActivations: Seq[DeviceActivationV2], v2Format: Boolean): JsObject = {
    Json.obj(
      dataField -> deviceActivations.map(a => deviceActivationToJson(a, v2Format)))
  }

  protected def deviceDefaultPrefsToJson(defaultPrefs: DefaultDevicePreferences): JsObject = {
    val updatedBy = defaultPrefs.updatedBy.map(u => entityDescriptorToRelationshipJson(u)).getOrElse(Json.obj())

    Json.obj(
      dataField -> Json.obj(
        agencyType -> defaultPrefs.partnerId,
        modelField -> defaultPrefs.model,
        defaultPrefs.preferences.map(toPreferencesJson).getOrElse(preferences -> Json.arr()),
        relationshipsField -> Json.obj(updatedByField -> updatedBy)))
  }

  protected def deviceInfoWithIncludesJson(device: DeviceMetaData, included: Option[JsValue] = None, isEdcaAudience: Boolean = false): JsObject = {
    val dataObj = createJsonDataObject(deviceInfoToJson(device, isEdcaAudience))

    val result =
      included.map { includedList =>
        dataObj ++ Json.obj(includedField -> includedList)
      }.getOrElse(dataObj)

    result
  }

  protected def deviceInfoToJson(device: DeviceMetaData, isEdcaAudience: Boolean = false): JsObject = {
    var attributes = Json.obj(
      deviceSerialField -> device.serial,
      statusField -> device.`status`.map(_.toString.toLowerCase),
      status2Field -> device.status2.map(_.toString.toLowerCase),
      makeField -> device.make,
      modelField -> device.model,
      friendlyNameField -> device.friendlyName,
      device.warranties.map(toWarrantiesJson).getOrElse(warrantiesField -> Json.arr()),
      warrantyExpiryDateTimeField -> getWarrantyExpiry(device.warranties),
      device.preferences.map(toPreferencesJson).getOrElse(preferences -> Json.arr()),
      device.errorsV2.map(toDeviceErrorsJson).getOrElse(deviceErrorsField -> Json.arr()),
      firmwareVersionField -> device.firmware.flatMap(_.displayVersion),
      deviceCanaryField -> device.canary,
      dateAssignedField -> device.dateAssigned,
      deviceHomeIdField -> device.deviceHome.flatMap(_.id),
      lastUploadDateTimeField -> device.dateLastUpload,
    )
    var relationships = Json.obj(
      ownerField -> device.owner.map(entityDescriptorToRelationshipJson),
      updatedByField -> device.updatedBy.map(entityDescriptorToRelationshipJson),
      agencyType -> device.partnerId.map(agencyIdToRelationshipJson)
    )
    if (isEdcaAudience) {
      attributes = attributes ++ Json.obj(
        device.lteInfo.map(toLteInfoJson).getOrElse(lteInfoField -> JsNull)
      )
      relationships = relationships ++ Json.obj(
        assignedByField -> device.assignedBy.map(entityDescriptorToRelationshipJson)
      )
    }
    Json.obj(
      typeField -> deviceType,
      idField -> device.id,
      attributesField -> attributes,
      relationshipsField -> relationships
    )
  }

  protected def deviceInfoToJsonArray(device: DeviceMetaData): JsArray = {
    Json.arr(deviceInfoToJson(device))
  }

  protected def deviceListJsonResponse(
    devices: Seq[DeviceMetaData],
    included: JsValue = Json.arr(),
    paginationResponseOpt: Option[PaginationResponse] = None)
  : JsObject = {
    Json.obj(
      dataField -> devices.map(deviceInfoToJson(_)),
      includedField -> included) ++ paginationJson(paginationResponseOpt)
  }

  protected def deviceLogInfoToJson(log: DeviceLogMetadata): JsObject = {
    val url = controllers.routes.Devices.downloadLog(log.deviceModel, log.serial, log.fileKey, log.uploadedAt).url

    Json.obj(
      typeField -> deviceLogType,
      idField -> log.fileKey,
      attributesField -> Json.obj(
        deviceLogKeyField -> log.fileKey,
        deviceLogUrlField -> url,
        deviceLogVersionField -> log.version,
        deviceLogFriendlyNameField -> log.friendlyName,
        deviceLogUploadedAt -> log.uploadedAt))
  }

  protected def deviceLogInfoToJsonArray(logs: Seq[DeviceLogMetadata]): JsArray = {
    JsArray(
      logs.map { log => deviceLogInfoToJson(log) })
  }

  protected def deviceWithErrorListJsonResponse(devicesWithErrorCode: Seq[DeviceWithErrorCode], devicesWithHttpStatusMetadata: Seq[DeviceWithHttpStatusMetadata], included: Option[JsValue] = None): JsObject = {
    val devicesWithErrorCodeJson: Seq[JsObject] = devicesWithErrorCode.map(deviceWithErrorCodeToJson)
    val devicesWithHttpStatusMetadataJson: Seq[JsObject] = devicesWithHttpStatusMetadata.map(deviceWithHttpStatusMetadataToJson)

    val dataObj =
      Json.obj(
        dataField -> (devicesWithErrorCodeJson ++ devicesWithHttpStatusMetadataJson)
      )

    val result =
      included.map { includedList =>
        dataObj ++ Json.obj(includedField -> includedList)
      }.getOrElse {
        dataObj
      }

    result
  }

  protected def errorResponse(
    resultStatus: Results.Status,
    title: Option[String] = None,
    detail: Option[String] = None,
    canonicalString: Option[String] = None,
    serviceErrorCode: Option[Int] = None
  ): Result = {
    resultStatus match {
      case Results.Conflict => {
        Conflict(
          errorToJson(
            httpStatus = CONFLICT,
            title = title.getOrElse(DuplicateString),
            detail = detail.getOrElse(DuplicateString),
            canonicalString = canonicalString.getOrElse(DuplicateCanonicalString),
            code = serviceErrorCode
          )
        )
      }
      case Results.InternalServerError => {
        InternalServerError(
          errorToJson(
            httpStatus = INTERNAL_SERVER_ERROR,
            title = title.getOrElse(InternalServerErrorString),
            detail = detail.getOrElse(InternalServerErrorString),
            canonicalString = canonicalString.getOrElse(InternalServerErrorCanonicalString),
            code = serviceErrorCode
          )
        )
      }
      case Results.BadRequest => {
        BadRequest(
          errorToJson(
            httpStatus = BAD_REQUEST,
            title = title.getOrElse(InvalidInputString),
            detail = detail.getOrElse(InvalidInputString),
            canonicalString = canonicalString.getOrElse((InvalidInputCanonicalString)),
            code = serviceErrorCode
          )
        )
      }
      case Results.NotFound => {
        NotFound(
          errorToJson(
            httpStatus = NOT_FOUND,
            title = title.getOrElse(ResourceNotFoundString),
            detail = detail.getOrElse(ResourceNotFoundString),
            canonicalString = canonicalString.getOrElse(ResourceNotFoundCanonicalString),
            code = serviceErrorCode
          )
        )
      }
      case Results.Forbidden => {
        Forbidden(
          errorToJson(
            httpStatus = FORBIDDEN,
            title = title.getOrElse(OperationNotPermittedString),
            detail = detail.getOrElse(OperationNotPermittedString),
            canonicalString = canonicalString.getOrElse(OperationNotPermittedCanonicalString),
            code = serviceErrorCode
          )
        )
      }
      case Results.Unauthorized => {
        Unauthorized(
          errorToJson(
            httpStatus = UNAUTHORIZED,
            title = title.getOrElse(UnauthorizedString),
            detail = detail.getOrElse(UnauthorizedString),
            canonicalString = canonicalString.getOrElse(UnauthorizedCanonicalString),
            code = serviceErrorCode
          )
        )
      }
      case Results.TooManyRequests => {
        TooManyRequests(
          errorToJson(
            httpStatus = TOO_MANY_REQUESTS,
            title = title.getOrElse(TooManyRequestsString),
            detail = detail.getOrElse(TooManyRequestsString),
            canonicalString = canonicalString.getOrElse(TooManyRequestsCanonicalString),
            code = serviceErrorCode
          )
        )
      }
      case _ => {
        InternalServerError(
          errorToJson(
            httpStatus = INTERNAL_SERVER_ERROR,
            title = title.getOrElse(InternalServerErrorString),
            detail = detail.getOrElse(InternalServerErrorString),
            canonicalString = canonicalString.getOrElse(InternalServerErrorCanonicalString),
            code = serviceErrorCode
          )
        )
      }
    }
  }

  protected def errorToJson(
    httpStatus: Int,
    title: String,
    detail: String,
    canonicalString: String,
    code: Option[Int] = None
  ): JsObject = {
    val codeProperty = code.map(c => Json.obj(codeField -> c)).getOrElse(Json.obj())

    Json.obj(
      errorsField -> Json.arr(
        Json.obj(
          statusField -> httpStatus,
          titleField -> title,
          detailField -> detail,
          canonicalStringField -> canonicalString
        ) ++ codeProperty))
  }

  protected def serviceErrorToJson(
                             httpStatus: Int,
                             title: String,
                             detail: String,
                             canonicalString: String,
                             code: Option[Int] = None
                           ): JsObject = {
    val codeProperty = code.map(c => Json.obj(codeField -> c)).getOrElse(Json.obj())

    Json.obj(
      serviceErrorField -> Json.arr(
        Json.obj(
          statusField -> httpStatus,
          titleField -> title,
          detailField -> detail,
          canonicalStringField -> canonicalString
        ) ++ codeProperty))
  }

  protected def fromPreferencesJson(data: JsValue): Option[Preference] = {
    val updatedOn = (data \ updatedField).asOpt[String].getOrElse(ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME))
    (data \ keyField).asOpt[String].map {
      key =>
        (data \ valueField).asOpt[String].map {
          value => Some(Preference(key, value, updatedOn))
        }.getOrElse(None)
    }.getOrElse(None)
  }

  protected def getEmptyListResponse(selfPaginationLink: String): Result = {
    Ok(
      Json.obj(
        linksField -> paginationJson(Some(selfPaginationLink)),
        dataField -> Json.arr()))
  }

  protected def getSolrDevices(r: SolrResponse): Seq[SolrDevice] = {
    val solrDevices =
      r.docs.flatMap { v =>
        (v \ "entity_domain").asOpt[String].map { partnerId =>
          (v \ "entity_id").asOpt[String].map { id =>

            val serial = (v \ "device_serial_number").asOpt[String]
            val status = (v \ "device_status").asOpt[String]
            val status2 = (v \ "device_status_2").asOpt[String]
            val errorStatus = (v \ "device_last_event").asOpt[String]
            val model = (v \ "device_model").asOpt[String]
            val friendlyName = (v \ "device_friendly_name").asOpt[String]
            val `type` = (v \ "device_type").asOpt[String]
            val firmwareVersion = (v \ "device_firmware_version").asOpt[String]
            val warrantyExpiryDateTime = (v \ "device_warranty_expiry_datetime").asOpt[String]
            val lastUploadDateTime = (v \ SolrConstants.DeviceLastUploadSolr).asOpt[String]
            val lastIssuedToTid = (v \ "device_last_issued_to_subscriber_tid").asOpt[String].flatMap(toEntityDescriptor)
            val owner = (v \ "entity_owner_tid").asOpt[String].flatMap(toEntityDescriptor)
            val updatedBy = (v \ "entity_updated_by_tid").asOpt[String].flatMap(toEntityDescriptor)
            val homeId = (v \ SolrConstants.DeviceHomeIdFieldSolr).asOpt[String].flatMap(Convert.tryToUuid)
            val dateCreated = (v \ "entity_date_created").asOpt[String]
            val dateModified = (v \ "entity_date_modified").asOpt[String]
            val ownerEvidenceGroup = (v \ "device_owner_evidence_group_id").asOpt[UUID]

            SolrDevice(
              partnerId = Convert.toGuidIfNecessary(partnerId),
              id = Convert.toGuidIfNecessary(id),
              serial = serial,
              status = status,
              status2 = status2,
              errorStatus = errorStatus,
              model = model,
              friendlyName = friendlyName,
              `type` = `type`,
              firmwareVersion = firmwareVersion,
              warrantyExpiryDateTime = warrantyExpiryDateTime,
              lastUploadDateTime = lastUploadDateTime,
              lastIssuedToTid = lastIssuedToTid,
              owner = owner,
              updatedBy = updatedBy,
              homeId = homeId,
              dateCreated = dateCreated,
              dateModified = dateModified,
              ownerEvidenceGroup = ownerEvidenceGroup)
          }
        }
      }.flatten

    solrDevices
  }

  protected def lteConfigToJson(lteConfig: Option[LteConfig], supportedLteCarriers: Option[Seq[LteCarrier]], overriddenLteCarriers: Option[Seq[LteCarrier]]): Option[JsObject] = {
    val supportedCarriers = supportedLteCarriers.getOrElse(Seq()).map(_.originalName)
    val overriddenCarriers = overriddenLteCarriers.getOrElse(Seq()).map(_.originalName)
    if (lteConfig.isEmpty && supportedCarriers.isEmpty) {
      None
    } else {
      val json = lteConfig.map { config =>
        Json.obj(
          dataField -> Json.obj(
            preferredLteCarrierField -> config.preferredLteCarrier.originalName,
            updateReasonField -> config.updateReason.toString,
            updatedByField -> entityDescriptorToRelationshipJson(config.updatedByTid),
            lastUpdatedTimestampField -> config.lastUpdatedTimestampMillis,
            supportedLteCarriersField -> supportedCarriers,
            overriddenLteCarriersField -> overriddenCarriers
          )
        )
      }.getOrElse{
        Json.obj(
          dataField -> Json.obj(
            supportedLteCarriersField -> supportedCarriers,
            overriddenLteCarriersField -> overriddenCarriers
          )
        )
      }
      Some(json)
    }
  }


  protected def networkConfigJsonGetResponse(config: NetworkConfig): JsObject = {
    Json.obj(
      dataField -> networkConfigInfoToJson(config, redactPass = true))
  }

  protected def networkConfigJsonListResponse(resp: ListNetworkConfigsResponse): Option[Result] = {
    for {
      configs <- resp.configs
    } yield {
      Ok(Json.obj(dataField -> configs.map(c => networkConfigInfoToJson(c, redactPass = true))))
    }
  }

  protected def paginationJson(self: Option[String] = None, first: Option[String] = None, prev: Option[String] = None, next: Option[String] = None): JsObject = {
    val s = self.map(s => ("self", Json.toJsFieldJsValueWrapper(s)))
    val f = first.map(f => ("first", Json.toJsFieldJsValueWrapper(f)))
    val p = prev.map(p => ("prev", Json.toJsFieldJsValueWrapper(p)))
    val n = next.map(n => ("next", Json.toJsFieldJsValueWrapper(n)))

    val obj: Seq[(String, JsValueWrapper)] = Seq(s, f, p, n).flatten
    Json.obj(obj: _*)
  }

  protected def partnerEntityDescriptor(partnerId: String): EntityDescriptor = {
    EntityDescriptor(TidEntities.Partner, partnerId, partnerId)
  }

  protected def rescueArsenalCalls[T](promise: com.twitter.util.Future[T], func: String, area: String): Future[Either[ServiceErrorCode, T]] = {
    rescueToScala(s"$func: $area:")(promise)(translator)
  }

  protected def signalConfigJsonListResponse(config: SignalConfig): JsObject = {
    Json.obj(
      dataField -> Json.arr(signalConfigToJson(config)))
  }

  protected def signalConfigJsonListResponseV2(config: SignalConfig): JsObject = {
    Json.obj(
      dataField -> Json.arr(signalConfigToJsonV2(config)))
  }

  protected def signalConfigToJson(config: SignalConfig): JsObject = {
    Json.obj(
      typeField -> signalConfigType,
      idField -> config.id,
      attributesField -> Json.obj(
        deviceModelField -> config.deviceModel,
        statusField -> config.status.get.toString.toLowerCase,
        config.gpioTriggerDefs.map(gpioTriggersToJson(_)).getOrElse(gpiosField -> Json.arr())),
      relationshipsField -> Json.obj(
        ownerField -> entityDescriptorToRelationshipJson(config.owner.get),
        updatedByField -> entityDescriptorToRelationshipJson(config.updatedBy.get),
        agencyType -> agencyIdToRelationshipJson(config.partnerId.get)))
  }

  protected def signalConfigToJsonV2(config: SignalConfig): JsObject = {
    Json.obj(
      typeField -> signalConfigType,
      idField -> config.id,
      attributesField -> Json.obj(
        deviceModelField -> config.deviceModel,
        statusField -> config.status.get.toString.toLowerCase,
        config.gpioTriggerDefs.map(gpioTriggersToJsonV2(_)).getOrElse(gpiosField -> Json.arr())),
      relationshipsField -> Json.obj(
        ownerField -> entityDescriptorToRelationshipJson(config.owner.get),
        updatedByField -> entityDescriptorToRelationshipJson(config.updatedBy.get),
        agencyType -> agencyIdToRelationshipJson(config.partnerId.get)))
  }

  protected def solrDeviceToJson(solrResponse: SolrResponse, solrDevices: Seq[SolrDevice], groups: Map[UUID,TeamDetails] = Map(), isV1: Boolean = true): Seq[JsObject] = {
    val devicesJson =
      solrDevices.map { solrDevice =>
        deviceJson(
          partnerId = Convert.toGuidIfNecessary(solrDevice.partnerId),
          id = Convert.toGuidIfNecessary(solrDevice.id),
          serial = solrDevice.serial,
          status = solrDevice.status,
          status2 = solrDevice.status2,
          errorStatus = solrDevice.errorStatus,
          model = solrDevice.model,
          friendlyName = solrDevice.friendlyName,
          `type` = solrDevice.`type`,
          firmwareVersion = solrDevice.firmwareVersion,
          warrantyExpiryDateTime = solrDevice.warrantyExpiryDateTime,
          lastUploadDateTime = solrDevice.lastUploadDateTime,
          lastIssuedToTid = solrDevice.lastIssuedToTid,
          owner = solrDevice.owner,
          updatedBy = solrDevice.updatedBy,
          homeId = solrDevice.homeId,
          dateCreated = solrDevice.dateCreated,
          dateModified = solrDevice.dateModified,
          ownerEvidenceGroup =  solrDevice.ownerEvidenceGroup.flatMap(groups.get),
          isV1 = isV1
        )
      }

    devicesJson
  }

  protected def solrVehiclesToJson(r: SolrResponse): JsObject = {
    val data = r.docs.flatMap { v =>
      (v \ "entity_domain").asOpt[String].map { partnerId =>
        (v \ "entity_id").asOpt[String].map { id =>
          val name = (v \ "vehicle_name").asOpt[String]
          val category = (v \ "vehicle_category").asOpt[String].flatMap(VehicleCategory.valueOf)
          val status = (v \ "vehicle_status").asOpt[String].flatMap(VehicleStatus.valueOf)
          val owner = (v \ "entity_owner_tid").asOpt[String].flatMap(toEntityDescriptor)
          val updatedBy = (v \ "entity_updated_by_tid").asOpt[String].flatMap(toEntityDescriptor)
          val dateCreated = (v \ "entity_date_created").asOpt[String]
          val dateModified = (v \ "entity_date_modified").asOpt[String]

          vehicleJson(
            Convert.toGuidIfNecessary(partnerId),
            Convert.toGuidIfNecessary(id),
            name = name,
            category = category,
            status = status,
            owner = owner,
            updatedBy = updatedBy,
            dateCreated = dateCreated,
            dateModified = dateModified
          )
        }
      }
    }
    Json.obj(dataField -> data)
  }

  protected def toEntityDescriptor(tidString: String): Option[EntityDescriptor] = {
    if (tidString.isEmpty)
      None
    else {
      val commonTid = CommonTid.parse(tidString)
      Option(
        EntityDescriptor(
          entityType = TidEntities.valueOf(toThriftEnumName(commonTid.entity)).get,
          id = commonTid.id.toString,
          partnerId = commonTid.domain.getOrElse("").toString))
    }
  }

  protected def updatedByFromJwt(jwtWrapper: JWTWrapper): EntityDescriptor = {
    val te = TidEntities.valueOf(jwtWrapper.subjectType).get
    EntityDescriptor(te, jwtWrapper.subjectId, jwtWrapper.subjectDomain.getOrElse(jwtWrapper.subjectId))
  }

  protected def validateAgencyId(agencyId: String): Either[Result, UUID] = {
    validateUuid(agencyId, s"AgencyId '${agencyId}' invalid")
  }

  protected def validateSubscriberId(agencyId: String): Either[Result, UUID] = {
    validateUuid(agencyId, s"SubscriberId '${agencyId}' invalid")
  }

  protected def validateDeviceHomeId(id: String): Either[Result, UUID] = {
    validateUuid(id, s"DeviceHomeId '${id}' invalid")
  }

  protected def validateFirmwareVersion(version: String): Either[Result, Unit] = {
    if (StringUtils.isNullOrWhiteSpace(version)) {
      Left(Results.BadRequest(s"Firmware version '${version}' invalid"))
    } else {
      Right(())
    }
  }

  protected def validateDeviceId(deviceId: String): Either[Result, UUID] = {
    validateUuid(deviceId, s"DeviceId '${deviceId}' invalid")
  }

  protected def validateUuid(value: String, errorMessage: String): Either[Result, UUID] = {
    Convert.tryToUuid(value)
      .toRight {
        errorResponse(resultStatus = Results.BadRequest, detail = Some(errorMessage))
      }
  }

  protected def validateUuid(value: Option[String], errorMessage: String): Either[Result, Option[UUID]] = {
    value.map { v =>
      Convert.tryToUuid(v) match {
        case Some(uuid) => Right(Some(uuid))
        case None => Left(errorResponse(resultStatus = Results.BadRequest, detail = Some(errorMessage)))
      }
    }.getOrElse {
      Right(None)
    }
  }

  protected def validateMessageLength(value: Option[String], length: Int): Either[Result, Option[String]] = {
    if (value.isDefined && value.get.length > length) {
      val errorMessage = s"Message exceeds max length of $length characters."
      Left(errorResponse(resultStatus = Results.BadRequest, detail = Some(errorMessage)))
    } else {
      Right(value)
    }
  }

  private def vehicleJson(
    partnerId: String,
    id: String,
    name: Option[String] = None,
    category: Option[VehicleCategory] = None,
    status: Option[VehicleStatus] = None,
    make: Option[String] = None,
    model: Option[String] = None,
    owner: Option[EntityDescriptor] = None,
    updatedBy: Option[EntityDescriptor] = None,
    dateCreated: Option[String] = None,
    dateModified: Option[String] = None): JsObject = {
    Json.obj(
      typeField -> vehicleType,
      idField -> id,
      attributesField -> Json.obj(
        vehicleNameField -> name,
        vehicleCategoryField -> category.get.toString.toLowerCase,
        statusField -> status.get.toString.toLowerCase,
        makeField -> make,
        modelField -> model,
        dateCreatedField -> dateCreated,
        dateModifiedField -> dateModified,
      ),
      relationshipsField -> Json.obj(
        ownerField -> Json.toJson(owner),
        updatedByField -> Json.toJson(updatedBy),
      )
    )
  }

  protected def vehicleCoreToJson(vehicle: VehicleMetadata): JsObject = {
    Json.obj(
      typeField -> vehicleType,
      idField -> vehicle.id,
      attributesField -> Json.obj(
        vehicleNameField -> vehicle.name,
        wirelessOffloadField -> vehicle.wirelessOffload,
        vehicleCategoryField -> vehicle.category.get.toString.toLowerCase,
        statusField -> vehicle.`status`.get.toString.toLowerCase,
        makeField -> vehicle.make,
        modelField -> vehicle.model,
        dateCreatedField -> vehicle.dateCreated,
        vehicleGenerationField -> vehicle.vehicleGeneration.getOrElse(VehicleGeneration.Fleet).toString,
        vehicleHomeIdField -> vehicle.vehicleHome.map(home => home.id),
        vehicleHomeNameField -> vehicle.vehicleHome.map(home => home.name),
        primaryHubSerialField -> vehicle.primaryHubSerial,
        primaryHubIdField -> vehicle.primaryHubId),
      relationshipsField -> Json.obj(
        ownerField -> entityDescriptorToRelationshipJson(vehicle.owner.get),
        updatedByField -> entityDescriptorToRelationshipJson(vehicle.updatedBy.get),
        devicesField -> vehicleDeviceRelationshipsToJson(vehicle),
        networkConfigsField -> vehicleNetworkConfigRelationshipsToJson(vehicle),
        fleetCameraConfigsField -> vehicleFleetCameraConfigRelationshipsToJson(vehicle)))
  }

  protected def vehicleJsonResponse(vehicle: VehicleMetadata): JsObject = {
    Json.obj(
      dataField -> vehicleCoreToJson(vehicle),
      includedField -> vehicleIncludesToJson(vehicle))
  }

  private def stringSequenceJsonResponse(stringSeq: Seq[String], jsonApiType: String): JsObject = {
    Json.obj(
      dataField -> JsArray(stringSeq.map { singleString =>
        Json.obj(
          typeField -> jsonApiType,
          idField -> singleString
        )
      })
    )
  }

  protected def vehicleNamesJsonResponse(vehicles: Seq[String]): JsObject = {
    stringSequenceJsonResponse(vehicles, vehicleNameField)
  }

  //todo: need to combine these along with devices and other entities into a common response path
  protected def vehicleListJsonResponse(vehicles: Seq[VehicleMetadata], paginationResponseOpt: Option[PaginationResponse] = None): JsObject = {
    Json.obj(
      dataField -> vehicles.map(vehicleCoreToJson),
      includedField -> vehicles.flatMap(vehicleIncludesToJson)) ++ paginationJson(paginationResponseOpt)
  }

  private def activationsToJson(activations: scala.collection.Map[DeviceActivationTrigger, SignalTriggerAction]): JsObject = {
    JsObject(
      activations
      .map(t => (t._1.triggerType.toString.toLowerCase, JsString(t._2.toString.toLowerCase))))
  }

  private def activationsToJsonV2(activations: scala.collection.Map[DeviceActivationTrigger, SignalTriggerAction]): JsArray = {
    JsArray(
      activations.map { case (trigger, action) => Json.obj(
        typeField -> trigger.triggerType.toString.toLowerCase,
        actionField -> action.toString.toLowerCase,
        highToLowField -> trigger.highToLowVoltage
    )}.toSeq)
  }

  private def agencyIdToRelationshipJson(agencyId: String): JsObject = {
    val entityDescriptor = EntityDescriptor(TidEntities.Partner, agencyId, agencyId)
    entityDescriptorToRelationshipJson(entityDescriptor)
  }

  private def arsenalErrorToHttpStatusMetadata(sec: ServiceErrorCode): HttpStatusMetadata = {
    arsenalErrorCodeToHttpStatusMetadataMap.getOrElse(
      sec,
      HttpStatusMetadata(
        resultStatus = Results.InternalServerError,
        httpStatus = INTERNAL_SERVER_ERROR,
        title = Some(InternalServerErrorString),
        detail = Some(InternalServerErrorString),
        canonicalString = InternalServerErrorCanonicalString
      )
    )
  }

  private def deviceActivationToJson(deviceActivation: DeviceActivationV2, v2Format: Boolean): JsObject = {
    val deviceActivations = if(v2Format) activationsToJsonV2(deviceActivation.activations) else activationsToJson(deviceActivation.activations)
    Json.obj(
      modelField -> deviceActivation.model,
      propsField -> devicePropsToJson(deviceActivation.props),
      activationsField -> deviceActivations)
  }

  private def deviceJson(
    partnerId: String,
    id: String,
    serial: Option[String] = None,
    status: Option[String] = None,
    status2: Option[String] = None,
    errorStatus: Option[String] = None,
    model: Option[String] = None,
    friendlyName: Option[String] = None,
    `type`: Option[String] = None,
    firmwareVersion: Option[String] = None,
    warrantyExpiryDateTime: Option[String] = None,
    lastUploadDateTime: Option[String] = None,
    lastIssuedToTid: Option[EntityDescriptor] = None,
    owner: Option[EntityDescriptor] = None,
    updatedBy: Option[EntityDescriptor] = None,
    homeId: Option[UUID] = None,
    dateCreated: Option[String] = None,
    dateModified: Option[String] = None,
    ownerEvidenceGroup: Option[TeamDetails] = None,
    isV1: Boolean
  ): JsObject = {

    implicit val w: Writes[TeamDetails] = teamDetailsNameOnlyWrites
    val ownerEvidenceGroupVal = if (isV1) None else ownerEvidenceGroup
    Json.obj(
      typeField -> deviceType,
      idField -> id,
      attributesField -> JsonWriteHelpers.withOptionalFields(Json.obj(
        deviceSerialField -> serial,
        statusField -> status,
        status2Field -> status2,
        errorStatusField -> errorStatus,
        typeField -> `type`,
        modelField -> model,
        friendlyNameField -> friendlyName,
        firmwareVersionField -> firmwareVersion,
        lastUploadDateTimeField -> lastUploadDateTime,
        warrantyExpiryDateTimeField -> warrantyExpiryDateTime,
        deviceHomeIdField -> homeId.map(_.toString),
        dateCreatedField -> dateCreated,
        dateModifiedField -> dateModified,
      ), ownerEvidenceGroupField -> ownerEvidenceGroupVal),
      relationshipsField -> Json.obj(
        ownerField -> owner.map(entityDescriptorToRelationshipJson),
        updatedByField -> updatedBy.map(entityDescriptorToRelationshipJson),
        lastIssuedToTidField -> lastIssuedToTid.map(entityDescriptorToRelationshipJson),
        agencyType -> agencyIdToRelationshipJson(partnerId)))
  }

  private def devicePropsToJson(props: Option[scala.collection.Map[String, String]]): JsObject = {
    JsObject(
      props
      .getOrElse(Map())
      .map(t => (t._1.toLowerCase, JsString(t._2.toLowerCase))))
  }

  private def deviceWithErrorCodeToJson(deviceWithError: DeviceWithErrorCode): JsObject = {
    val httpStatusMetadata = arsenalErrorToHttpStatusMetadata(deviceWithError.errorCode)
    val deviceWithHttpStatusMetadata = DeviceWithHttpStatusMetadata(id = deviceWithError.deviceId, device = deviceWithError.device, httpStatusMetadata = httpStatusMetadata)

    deviceWithHttpStatusMetadataToJson(deviceWithHttpStatusMetadata)
  }

  private def deviceWithErrorInfoToJson(device: DeviceMetaData, httpStatusMetadata: HttpStatusMetadata) = {
    deviceInfoToJson(device) ++
    Json.obj(
      idField -> device.id,
      statusField -> device.`status`.map(_.toString.toLowerCase),
      status2Field -> device.status2.map(_.toString.toLowerCase),
      canonicalStringField -> httpStatusMetadata.canonicalString
    )
  }

  private def deviceWithHttpStatusMetadataToJson(deviceWithHttpStatusMetadata: DeviceWithHttpStatusMetadata): JsObject = {
    val deviceMetadata =
      deviceWithHttpStatusMetadata.device.getOrElse {
        DeviceMetaData(id = Some(deviceWithHttpStatusMetadata.id))
      }

    deviceWithErrorInfoToJson(deviceMetadata, deviceWithHttpStatusMetadata.httpStatusMetadata)
  }

  private def entityDescriptorToLinkJson(entityDescriptor: EntityDescriptor): JsObject = {
    val agencyEntity = agencyType -> entityDescriptor.partnerId

    Json.obj(
      entityDescriptor.entityType match {
        case TidEntities.Partner => selfField -> entityUrl(agencyEntity)
        case _ => selfField -> entityUrl(agencyEntity, entityDescriptor.entityType -> entityDescriptor.id)
      })
  }

  private def entityDescriptorToRelationshipJson(entityDescriptor: EntityDescriptor): JsObject = {
    Json.obj(
      dataField -> Json.obj(
        typeField -> JsonApiOrgToThriftV1.entityTypeToJson(entityDescriptor.entityType),
        idField -> entityDescriptor.id),
      linksField -> entityDescriptorToLinkJson(entityDescriptor))
  }

  private def fleetCameraConfigToJson(config: FleetCameraConfig): JsObject = {
    Json.obj(
      typeField -> fleetCameraConfigType,
      idField -> config.mountId,
      attributesField -> Json.obj(
        activeUsersField -> config.activeUsers,
        configField -> config.configuration))
  }

  private def getWarrantyExpiry(warranties: Option[Seq[Warranty]]): Option[String] = {
    warranties match {
      case None => None
      case Some(ws) => {
        val expiryDates = ws.map(w => ZonedDateTime.parse(w.expirationDate, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
        .sortWith((left, right) => ChronoUnit.MINUTES.between(left, right) > 0)

        expiryDates.lastOption.map(e => e.toString)
      }
    }
  }

  private def gpioTriggersToJson(gpios: Seq[GpioTriggerDef]): (String, JsValueWrapper) = {
    gpiosField ->
    JsArray(
      gpios.map {
        gpio =>
          Json.obj(
            idField -> gpio.id,
            numberField -> gpio.number,
            typeField -> gpio.triggerType.get.toString.toLowerCase)
      })
  }

  private def gpioTriggersToJsonV2(gpios: Seq[GpioTriggerDef]): (String, JsValueWrapper) = {
    gpiosField ->
    JsArray(
      gpios.map {
        gpio =>
          Json.obj(
            idField -> gpio.id,
            numberField -> gpio.number,
            typeField -> gpio.triggerType.get.toString.toLowerCase,
            highToLowField -> gpio.highToLowVoltage.getOrElse(false).toString)
      })
  }

  private def networkConfigInfoToJson(config: NetworkConfig, redactPass: Boolean = false): JsObject = {
    val password = if(redactPass) Some("") else config.passphrase
    val location = config.location.getOrElse("")
    Json.obj(
      typeField -> networkConfigType,
      idField -> config.id,
      attributesField -> Json.obj(
        priority -> config.priority,
        typeFieldDeprecated -> config.`type`.get.toString, // TODO: Remove once all consumers have been updated.
        networkConfigTypeField -> config.`type`.get.toString,
        allowUploads -> config.allowUploads,
        ssid -> config.ssid,
        passphrase -> password,
        security -> config.security.map(_.toString),
        smartRouter -> config.smartRouter.getOrElse(false).toString.toBoolean,
        locationField -> location),
      dateModifiedField -> config.dateModified,
      relationshipsField -> Json.obj(
        ownerField -> entityDescriptorToRelationshipJson(config.owner.get),
        updatedByField -> entityDescriptorToRelationshipJson(config.updatedBy.get),
        agencyType -> agencyIdToRelationshipJson(config.partnerId.get)))
  }

  private def paginationJson(paginationResponseOpt: Option[PaginationResponse]) = {
    paginationResponseOpt.map { pagination =>
      Json.obj(
        metaField -> Json.obj(
          offsetField -> pagination.offset,
          limitField -> pagination.limit,
          countField -> pagination.count))
    }.getOrElse(Json.obj())
  }

  private def toDeviceErrorsJson(errors: Seq[ErrorInfoV2]): (String, JsValueWrapper) = {
    deviceErrorsField ->
    errors.map(error =>
      Json.obj(
        severityField -> error.severity.toString,
        codeField -> error.code,
        timestampField -> error.timestamp
      ))
  }

  private def toPreferencesJson(prefs: Seq[Preference]): (String, JsValueWrapper) = {
    preferences ->
    prefs.map(pref =>
      Json.obj(
        keyField -> pref.key,
        valueField -> pref.value,
        updatedField -> pref.dateModified))
  }

  private def toThriftEnumName(value: String) = {
    value.replace("_", "")
  }

  private def toLteInfoJson(lteInfo: LteInfo): (String, JsValueWrapper) = {
    lteInfoField -> Json.obj(
      assignedCarrierField -> lteInfo.assignedCarrier.map(_.toString),
      modemModelField -> lteInfo.modemModel.map(_.toString),
      imeiField -> lteInfo.imei,
      iccid1Field -> lteInfo.iccid1,
      iccid2Field -> lteInfo.iccid2
    )
  }

  private def toWarrantiesJson(warranties: Seq[Warranty]): (String, JsValueWrapper) = {
    warrantiesField ->
    warranties.map(warranty =>
      Json.obj(
        typeField -> warranty.`type`.toString,
        startDateField -> warranty.startDate,
        expirationDateField -> warranty.expirationDate))
  }

  private def vehicleDeviceRelationshipsToJson(vehicle: VehicleMetadata): JsObject = {
    vehicle.devices.map(devices =>
      Json.obj(
        dataField ->
        devices.map(d =>
          Json.obj(
            typeField -> deviceType,
            idField -> d.id)))).getOrElse(Json.obj())
  }

  private def vehicleFleetCameraConfigRelationshipsToJson(vehicle: VehicleMetadata): JsObject = {
    vehicle.fleetCameraConfigs.map(configs =>
      Json.obj(
        dataField ->
        configs.map(c =>
          Json.obj(
            typeField -> fleetCameraConfigType,
            idField -> c.mountId)))).getOrElse(Json.obj())
  }

  private def vehicleIncludesToJson(vehicle: VehicleMetadata): Seq[JsObject] = {
    val devices = vehicle.devices.map(devices => devices.map(d => deviceInfoToJson(d))).getOrElse(Nil)
    val networkConfigs = vehicle.networkConfigs.map(configs => configs.map(c => networkConfigInfoToJson(c))).getOrElse(Nil)
    val fleetCameraConfigs = vehicle.fleetCameraConfigs.map(configs => configs.map(c => fleetCameraConfigToJson(c))).getOrElse(Nil)

    devices ++ networkConfigs ++ fleetCameraConfigs
  }

  private def vehicleNetworkConfigRelationshipsToJson(vehicle: VehicleMetadata): JsObject = {
    vehicle.networkConfigs.map(configs =>
      Json.obj(
        dataField ->
        configs.map(c =>
          Json.obj(
            typeField -> networkConfigType,
            idField -> c.id)))).getOrElse(Json.obj())
  }

  protected case class DeviceWithHttpStatusMetadata(id: String, device: Option[DeviceMetaData], httpStatusMetadata: HttpStatusMetadata)

  protected case class HttpStatusMetadata(
    resultStatus: Results.Status,
    httpStatus: Int,
    title: Option[String] = None,
    detail: Option[String] = None,
    canonicalString: String,
    serviceErrorCode: Option[Int] = None
  )

}
