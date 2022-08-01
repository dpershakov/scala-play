package controllers.devices

import com.evidence.service.common.data.device.Model._
import controllers.devices.DeviceConstants.Models.{AxonBody3Model, AxonFleet3DualViewCamModel, AxonFleet3HubModel, AxonFleet3InteriorCamModel}

import java.util.concurrent.TimeUnit

object DeviceConstants {

  object DeletionMessage {
    final val Approved = "approved"
    final val ChecksumNotFound = "checksum not found"
    final val DeviceNotFoundForFile = "no device found for file"
    final val DeviceStatusMissing = "Missing device status"
    final val FileNotFound = "file not found"
    final val FileSignatureParsingError = "error parsing file signature"
    final val InternalServerError = "internal server error"
    final val InvalidAgencyId = "invalidAgencyId"
    final val InvalidEvidenceStatus = "invalid evidence status"
    final val EvidenceHasMoreThanOneFile = "evidence has more than one file"
    final val InvalidDeviceStatus = "invalid device status"
    final val InvalidFileSignature = "invalid file signature"
    final val InvalidIssuedTime = "invalid issued time"
    final val InvalidJwtSignature = "invalid jwt signature"
    final val InvalidRequest = "invalid request"
    final val SerialNumberMismatch = "serial number mismatch"
  }

  object LogConstants {
    final val DeviceModelQueryParam = "deviceModel"
    final val PartnerSearchType = "partner"
    final lazy val AgencyIdQueryParam = AgencyIdString
    final lazy val EndDateQueryParam = EndDateString
    final lazy val FileKeyQueryParam = FileKey
    final lazy val FileKeySearchType = FileKey.toLowerCase
    final lazy val SerialQueryParam = Serial
    final lazy val SerialSearchType = Serial
    final lazy val StartDateQueryParam = StartDateString
    private final val FileKey = "fileKey"
    private final lazy val Serial = SerialString
  }

  object Models {
    final val AxonBody2Model = AxonBody2.toString
    final val AxonBody3Model = AxonBody3.toString
    final val AxonDockModel = AxonDock.toString
    final val AxonFleet3DualViewCamModel = AxonFleet3DualViewCam.toString
    final val AxonFleet3InteriorCamModel = AxonFleet3InteriorCam.toString
    final val AxonFleet3HubModel = AxonFleet3Hub.toString
    final val AxonFlex2Model = AxonFlex2.toString
    final val Taser7HandleModel = Taser7Handle.toString
    final val Taser7HandleSingleLaserShortRangeModel = Taser7CQ.toString
    final val AxonFleet3DashboardModel = "axon_fleet_dashboard_windows"
    final val CaptureAndroidModel = "axon_capture_android"
    final val CaptureIosModel = "axon_capture_ios"
  }

  object TaserIndexPrefixes {
    final val Taser7 = "taser7"
    final val Taser10 = "taser10"
  }

  object DownloadFileNames {
    final val AxonFleet3DashboardInstaller = "Axon_Dashboard_Installer.msi"
  }

  object Ab3SshPayloadKeys {
    final val KeyIdString = "KeyId"
    final val DevPubKeyString = "DevPubKey"
    final val IVAesString = "IV_AES"
    final val PayloadString = "Payload"
    final val MACString = "MAC"
  }

  object DockSshPayloadKeys {
    final val KeyIdString = "KeyId"
    final val PayloadString = "Payload"
    final val SessionKeyString = "SessionKey"
    final val KeyDataString = "KeyData"
    final val IVString = "IV"
  }

  object RegistrationStatus {
    final val Registered = "REGISTERED"
    final val DeRegistered = "DEREGISTERED"
  }

  object RegistrationStatusRespPayloadKeys {
    final val Serial = "serial"
    final val AgencyDomain = "agencyDomain"
    final val AgencyUuid = "agencyUuid"
    final val Status = "status"
    final val IsFirstRegister = "isFirstRegistration"
  }

  object DeviceConfig {
    final val DockBasedRegistrationConfig = "automatic_camera_registration"
  }

  final val DefaultArtifactType = "device_firmware"
  final val DefaultSubType = "main"

  final val AgencyIdString = "agencyId"
  final val AliasString = "alias"
  final val Arm = "arm"
  final val ArtifactTypeString = "artifactType"
  final val AttributesString = "attributes"
  final val BaseDateTimeString = "baseDateTime"
  final val BatteryInserted = "battery_inserted"
  final val BatterySerialNumberString = "batterySerialNumber"
  final val BleChirpString = "bleChirp"
  final val CameraStateString = "state"
  final val CameraString = "camera"
  final val CewString = "cew"
  final val Cold = "cold"
  final val CommaChar = ','
  final val ConnectionString = "connection"
  final val CountString = "count"
  final val DataString = "data"
  final val DateTimeEndString = "dateTimeEnd"
  final val DateTimeStartString = "dateTimeStart"
  final val DeviceFirmwareString = "deviceFirmware"
  final val DeviceHomeIdLimit = 100
  final val DeviceIdString = "deviceId"
  final val DeviceModelString = "deviceModel"
  final val DeviceSerialString = "deviceSerial"
  final val DisplayNameString = "displayName"
  final val DockStateBase64EncKey = "state_b64enc"
  final val EmptyJsonString = "{}"
  final val EmptyString = ""
  final val EndDateString = "endDate"
  final val ErrorMask = "errorMask"
  final val ErrorString = "error"
  final val EvidenceIdString = "evidenceId"
  final val ExtractEvidenceString = "extractEvidence"
  final val ExtraPenetration = "extra_penetration"
  final val FieldsString = "fields"
  final val FieldString = "field"
  final val FilterTypesString = "filterTypes"
  final val FirmwareVersionString = "firmwareVersion"
  final val FuncString = "func"
  final val BulkDeviceAssignLimit = 100
  final val BulkDeviceAssignExceedLimitResponse = "Device Assignment exceeds Limit"
  final val Halt = "halt"
  final val Holster = "holster"
  final val IdString = "id"
  final val Inert = "inert"
  final val IsOnlineString = "isOnline"
  final val LastFunctionalTestEpoch = "lastFunctionalTestEpoch"
  final val ListString = "list"
  final val MessageString = "message"
  final val MetaString = "meta"
  final val ModelString = "model"
  final val None = "none"
  final val NoTypeString = "noType"
  final val OwnerEvidenceGroup  = "ownerEvidenceGroup"
  final val OwnerIdString = "ownerId"
  final val OwnerString = "owner"
  final val OwnerTypeString = "ownerType"
  final val PageLimitString = "pageLimit"
  final val PageNumberString = "pageNumber"
  final val PartnerIdString = "partnerId"
  final val PivotString = "pivot"
  final val PulseGraphTypeString = "pulseGraph"
  final val PulseGraphV2TypeString = "pulseGraphV2"
  final val RedirectToDownloadString = "redirectToDownload"
  final val RelationshipsString = "relationships"
  final val RequestString = "request"
  final val RequestTypeString = "requestType"
  final val SequenceNumberBeginString = "sequenceNumberBegin"
  final val SequenceNumberEndString = "sequenceNumberEnd"
  final val SerialPrefixString = "serialPrefix"
  final val SerialString = "serial"
  final val SessionNumberBeginString = "sessionNumberBegin"
  final val SessionNumberEndString = "sessionNumberEnd"
  final val SessionNumberString = "sessionNumber"
  final val SessionsTypeString = "sessions"
  final val ShareCewLogMaxMessageLength = 3000
  final val ShareCewLogTypeString = "shareCewLog"
  final val Standard = "standard"
  final val StartDateString = "startDate"
  final val SubTypeString = "subType"
  final val SwitchEnergize = "switch_energize"
  final val Taser10HandleStatePrefix = "Taser10State"
  final val Taser7BatteryStatePrefix = "Taser7BatteryState"
  final val Taser7ConfigTypeString = "taser7Config"
  final val Taser7HandleStatePrefix = "Taser7State"
  final val TaserConfigTypeString = "taserConfig"
  final val Test = "test"
  final val TimestampString = "timestamp"
  final val TitleString = "title"
  final val Training = "training"
  final val TriggerEnergize = "trigger_energize"
  final val TriggerPulled = "trigger_pulled"
  final val TypeString = "type"
  final val Unholster = "unholster"
  final val Unknown = "unknown"
  final val UpdateCewEventOwnerTypeString = "updateCewEventOwner"
  final val UpdateStateTypeString = "updateState"
  final val ValueString = "value"
  final val Vr = "virtual_reality"
  final val WeaponWarnStarted = "weapon_warn_started"
  final val WeaponArmedRaised = "weapon_armed_raised"
  final val WeaponDartDeployment = "weapon_dart_deployment"
  final val WeaponDisarmedRaised = "weapon_disarmed_raised"
  final val WeaponEnergizeStarted = "weapon_energize_started"
  final lazy val Taser7BatteryStateHandleLastSeenEpochKey = s"${Taser7BatteryStatePrefix}-HandleLastSeenEpoch"
  final lazy val Taser7BatteryStateHandleSerialKey = s"${Taser7BatteryStatePrefix}-HandleSerial"
  final lazy val HandleStateBatteryLastSeenEpochSuffix = "-BatteryLastSeenEpoch"
  final lazy val HandleStateBatterySerialSuffix = "-BatterySerial"
  final lazy val HandleStateLastFunctionalTestEpochSuffix = "-LastFunctionalTestEpoch"

  final val FriendlyNameMaxLength = 512
  final val FriendlyNameMinLength = 2

  final val BulkUpdateLimit = 100

  final val ValidDockBasedRegistrationTimeWindow = TimeUnit.DAYS.toMillis(90)

  final val ModelToDownloadName: Map[String, String] = Map(
    Models.AxonFleet3DashboardModel -> DownloadFileNames.AxonFleet3DashboardInstaller,
  )

  //List of prefixes used in CEW device model names
  final val CewDevicesModelPrefix = List(
    "taser_ecd",
    "taser_cam",
    "taser_cew",
    "taser_etm_flex_2" // full AxonDock model string used to exclude AxonFlexEtm
  )

  //List of prefixes used in Air device model names
  final val AirDevicesModelPrefix = List(
    AxonAir.toString
  )

  final val SupportedYuleModels = List(Taser7Handle, Taser7CQ, Taser10Handle, Taser10HandleVr)

  final val ModelsWithDeviceStatesInElasticsearch = Set(AxonBody3Model)

  final val ValidFilterTypes = Set(
    Arm,
    Test,
    BatteryInserted,
    Holster,
    Unholster,
    SwitchEnergize,
    TriggerEnergize,
    WeaponArmedRaised,
    WeaponDisarmedRaised,
    WeaponEnergizeStarted,
    WeaponWarnStarted,
    TriggerPulled,
    WeaponDartDeployment,
    Vr,
    Halt,
    Standard,
    ExtraPenetration,
    Training,
    Cold,
    Inert,
    Unknown,
    None,
  )

  final val DeviceNotFoundErrorMessage = "Device not found."
  final val DeviceRestrictedErrorMessage = "Devices of this model cannot be accessed through this API."

  final val SnfFilterSet = Set(AxonSnfServer.toString)
  final val MaxEtmFlex2StateLengthAllowed: Int = 25 * 1024 // 25 Kilo Bytes


  def isConnectedModel(model: String): Boolean = {
    // Fleet cameras might not be directly connected, but they are still connected in the sense of
    // receiving updates through websockets (just tunneled through the hub)
    model == AxonBody3Model || model == AxonFleet3HubModel || model == AxonFleet3DualViewCamModel ||
      model == AxonFleet3InteriorCamModel || model == AxonBody4.toString()
  }

  final val agencyConfigurableModels = List(
    AxonBody1.toString,
    AxonBody2.toString,
    AxonBody3.toString,
    AxonBody4.toString,
    AxonFleetFront.toString,
    AxonFleet2CamFront.toString,
    TaserFleetWirelessMic.toString,
    AxonFleetRear.toString,
    AxonFleet2CamRear.toString,
    AxonFleet3DualViewCam.toString,
    AxonFleet3InteriorCam.toString,
    AxonFleet3Hub.toString,
    AxonFlex1.toString,
    AxonFlex2.toString,
  )

  final val fleetAgencySettingsGuid = "00000000-0000-0000-0000-0000000F1EE7"


  object DevicePreferencesMetadata {
    //Fleet 1 & 2 settings
    final val reachabilityCheck = "reachabilityCheck"
    final val autoOffloadTime = "autoOffloadTime"
    final val portableMediaOffload = "portableMediaOffload"

    //AB3 settings
    final val ab3viewxloffload = "viewxlab3offload"

    final val fleetSettingsSet = Set(
      reachabilityCheck,
      autoOffloadTime,
      portableMediaOffload
    )

    final val ab3SettingsSet = Set(
      ab3viewxloffload
    )

    final val settingsSet = fleetSettingsSet ++ ab3SettingsSet

    def contains(setting: String): Boolean = {
      settingsSet.contains(setting)
    }

    def deviceModel(preferenceKeys: Seq[String]): Option[String] = {
      if (preferenceKeys.exists(ab3SettingsSet.contains(_))) Some(AxonBody3.toString())
      else if (preferenceKeys.exists(fleetSettingsSet.contains(_))) Some(AxonFleet2CamFront.toString())
      else scala.None
    }
  }
}
