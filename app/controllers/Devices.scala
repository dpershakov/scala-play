package controllers

import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.UUID
import com.evidence.api.thrift.v1
import com.evidence.api.thrift.v1.{Tid, TidEntities}
import com.evidence.common.protos.v1.entity.{Tid => ProtoTid}
import com.evidence.service.arkham.api.thrift.{ServiceErrorCode => ArkhamErrorCode, _}
import com.evidence.service.arsenal.thrift.{DeviceHome, ServiceErrorCode, DeviceIdWithStatus => ThriftDeviceIdWithStatus, _}
import com.evidence.service.audit.{Tid => AuditTid}
import com.evidence.service.common.auth.SessionDataKeys
import com.evidence.service.common.auth.jwt.ECOMScopes
import com.evidence.service.common.config.Configuration
import com.evidence.service.common.data.device.Model
import com.evidence.service.common.data.device.Model._
import com.evidence.service.common.finagle.RequestInfo
import com.evidence.service.common.logging.Logger.LogVariable
import com.evidence.service.common.logging.{LazyLogging, LoggingHelper}
import com.evidence.service.common.monad.FutureEither
import com.evidence.service.common.monitoring.statsd.StrictStatsD
import com.evidence.service.common.{Convert, StringUtils}
import com.evidence.service.komrade.thrift.{KomradeServiceErrorCode, User, EntityDescriptor => KomradeEntityDescriptor}
import com.evidence.service.panoptes.thrift.ReindexDeviceOwnerRequest
import com.evidence.service.reporter.api.thrift.v1.{GenerateReportRequest, ReportFileFormat, ReportType}
import com.evidence.service.sessions.api.thrift.v1.{CreateSessionResponse, SessionsServiceErrorCode}
import com.evidence.service.sherlock.thrift.ServiceCode.{ConnectionNotFound, Transmitted}
import com.evidence.service.sherlock.thrift.{EvidenceOffloadRequest, EvidenceOffloadRequestType, PushAdminCmdResponse, PushAdminCmdToConnectedDevicesRequest, Target}
import controllers.devices.DeviceConstants.Models._
import controllers.devices.DeviceConstants.LogConstants
import controllers.devices.DeviceConstants.BulkDeviceAssignLimit
import controllers.devices.DeviceConstants.BulkDeviceAssignExceedLimitResponse
import controllers.devices._
import controllers.devices.arkham.ArkhamHelpers
import controllers.devices.audit.{AuditEvent, PartnerAdminCommandSent, SubscriberAdminCommandSent, UserSshPasswordDecryptionStarted}
import controllers.devices.helpers.{DeviceConfigHelper, DeviceControllerHelper}
import controllers.devices.BulkDeviceAssignResponse.createBulkAssignResponseWriter

import javax.inject.Inject
import org.apache.kafka.clients.producer.RecordMetadata
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import play.api.libs.json.Format.GenericFormat
import play.api.libs.json._
import play.api.libs.ws.WSClient
import play.api.mvc._
import security.Security
import services.arkham._
import services.arsenal._
import services.arsenal.objects.firmwareInfo.GetAgencyFirmwareVersionsResponse
import services.audit.{AuditClient, AuditConversions}
import services.kafka.KafkaProducerRegistry
import services.komrade._
import services.komrade.KomradeJson.{userWritesMinimal, userWritesMinimalV2}
import services.panoptes.PanoptesClient
import services.sessions.{EntityMetadata, SessionsClient, TokenMetadata}
import services.sherlock.SherlockClient
import services.solr.SolrConstants._
import services.solr._
import utils._
import utils.common.Constants.rootPartnerIdTidFormat
import utils.common.{Base64, Constants}
import utils.data.ByteBufferOps._
import utils.data.PartnerID
import utils.devices.{DeviceHomeJson, DeviceJWT, DockBasedRegisterJWTParser, ModelGroupingUtils}
import utils.jsonapi.JsonApiHelpers.{badRequest, errorResult}
import utils.packages.InternalServerException
import utils.permissions._
import utils.search._

import java.util
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.ByteString
import utils.jsonapi.JsonReadHelpers.validJsOrBadRequest

import scala.collection.{JavaConverters, Seq, mutable}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class Devices @Inject()(
  arkham: ArkhamClient,
  val arsenal: ArsenalClient,
  authenticateViaToken: AuthenticateViaToken,
  audit: AuditClient,
  components: ControllerComponents,
  komrade: KomradeClient,
  domainNameResolver: DomainNameResolver,
  teamProvider: TeamDetailsProvider,
  panoptes: PanoptesClient,
  parse: PlayBodyParsers,
  reports: Reports,
  sessions: SessionsClient,
  val sherlock: SherlockClient,
  solrClient: SolrClient,
  wsClient: WSClient,
  kafka: KafkaProducerRegistry
)(implicit ec: ExecutionContext)
extends AbstractController(components)
with DeviceHomeJson
with AuditConversions
with ArsenalConversions
with ErrorResponseHelper
with Base64
with PresignedUrlTransformer
with PermissionsHelper
with PermissionsValidation
with ArkhamHelpers
with LazyLogging
with LoggingHelper
with StrictStatsD
with ControllerPermissionsHelper
with DeviceConfigHelper
with DeviceControllerHelper {

  private[this] val security = Security(authenticateViaToken)

  private val config = Configuration.load()
  private val adminCommandMaxBytes = config.getInt("device.adminCommandMaxBytes")
  private val adminCommandsConfig = config.getObject("device.adminCommands").entrySet().asScala.map { model =>
    val commandsList = model.getValue.unwrapped().asInstanceOf[util.ArrayList[String]]
    val commands = JavaConverters.asScalaBuffer(commandsList).toSeq
    model.getKey -> commands
  }.toMap

  // This map lists all supported adm models with the model ID as the Map key and the display name as the value
  private final val supportedAdmModels = {
    val supportedModels = mutable.Map.empty[String, AdmModelMetadata]
    supportedModels += AxonBody2.toString -> AdmModelMetadata(displayName = "Axon Body 2", alias = Nil, serialPrefix = AxonBody2.masks)
    supportedModels += AxonFlex2.toString -> AdmModelMetadata(displayName = "Axon Flex 2", alias = Seq("Flex2"), serialPrefix = AxonFlex2.masks)
    supportedModels += AxonFlex2Controller.toString -> AdmModelMetadata(displayName = "Axon Flex 2 Controller", alias = Nil, serialPrefix = Nil)
    supportedModels += AxonFlex2ControllerV2.toString -> AdmModelMetadata(displayName = "Axon Flex 2 Controller v2", alias = Nil, serialPrefix = Nil)
    supportedModels += AxonSidearm.toString -> AdmModelMetadata(displayName = "Signal Sidearm", alias = Nil, serialPrefix = AxonSidearm.masks)
    supportedModels += TaserX26pHandle.toString -> AdmModelMetadata(displayName = "TASER X26P", alias = Nil, serialPrefix = TaserX26pHandle.masks)
    supportedModels += AxonDock.toString -> AdmModelMetadata(displayName = "Axon Dock", alias = Nil, serialPrefix = AxonDock.masks)
    supportedModels += Sppm.toString -> AdmModelMetadata(displayName = "SPPM", alias = Seq("taser_sppm"), serialPrefix = Sppm.masks)
    supportedModels += TaserX2Handle.toString -> AdmModelMetadata(displayName = "Taser X2", alias = Nil, serialPrefix = TaserX2Handle.masks)
    // taser7 models
    supportedModels += Taser7Handle.toString -> AdmModelMetadata(displayName = "TASER 7", alias = Nil, serialPrefix = Taser7Handle.masks)
    supportedModels += Taser7CQ.toString -> AdmModelMetadata(displayName = "TASER 7 CQ", alias = Nil, serialPrefix = Taser7CQ.masks)
    supportedModels += Taser7Battery.toString -> AdmModelMetadata(displayName = "TASER 7 Battery", alias = Seq("taser_cew_7_battery_non_rechargeable"), serialPrefix = Taser7Battery.masks)
    supportedModels += Taser7BatteryCorrectional.toString -> AdmModelMetadata(displayName = "TASER 7 Disconnect Battery", alias = Nil, serialPrefix = Taser7BatteryCorrectional.masks)
    supportedModels += Taser7Cartridge3.toString -> AdmModelMetadata(displayName = "TASER 7 Live 3 Cartridge", alias = Nil, serialPrefix = Taser7Cartridge3.masks)
    supportedModels += Taser7TrainingCartridge3.toString -> AdmModelMetadata(displayName = "TASER 7 HALT 3 Cartridge", alias = Nil, serialPrefix = Taser7TrainingCartridge3.masks)
    supportedModels += Taser7InertCartridge3.toString -> AdmModelMetadata(displayName = "TASER 7 Resettable 3 Cartridge", alias = Nil, serialPrefix = Taser7InertCartridge3.masks)
    supportedModels += Taser7SimulationCartridge3.toString -> AdmModelMetadata(displayName = "TASER 7 Live Simulation 3 Cartridge", alias = Nil, serialPrefix = Taser7SimulationCartridge3.masks)
    supportedModels += Taser7VrCartridge3.toString -> AdmModelMetadata(displayName = "TASER 7 VR 3 Cartridge", alias = Nil, serialPrefix = Taser7VrCartridge3.masks)
    supportedModels += Taser7Cartridge12.toString -> AdmModelMetadata(displayName = "TASER 7 Live 12 Cartridge", alias = Nil, serialPrefix = Taser7Cartridge12.masks)
    supportedModels += Taser7TrainingCartridge12.toString -> AdmModelMetadata(displayName = "TASER 7 HALT 12 Cartridge", alias = Nil, serialPrefix = Taser7TrainingCartridge12.masks)
    supportedModels += Taser7InertCartridge12.toString -> AdmModelMetadata(displayName = "TASER 7 Resettable 12 Cartridge", alias = Nil, serialPrefix = Taser7InertCartridge12.masks)
    supportedModels += Taser7SimulationCartridge12.toString -> AdmModelMetadata(displayName = "TASER 7 Live Simulation 12 Cartridge", alias = Nil, serialPrefix = Taser7SimulationCartridge12.masks)
    supportedModels += Taser7VrCartridge12.toString -> AdmModelMetadata(displayName = "TASER 7 VR 12 Cartridge", alias = Nil, serialPrefix = Taser7VrCartridge12.masks)
    // ab3
    supportedModels += AxonBody3Model -> AdmModelMetadata(displayName = "Axon Body 3", alias = Nil, serialPrefix = Seq("1A1", "1B1", "1C1", "1D1", "2A1", "2B1", "2C1", "2D1", "5A0", "3A7", "4A9", "X60", "X73"))
    supportedModels += AxonBody3Dock1Bay.toString -> AdmModelMetadata(displayName = "Axon Body 3 Dock - 1 Bay", alias = Nil, serialPrefix = AxonBody3Dock1Bay.masks)
    supportedModels += AxonBody3Dock8Bay.toString -> AdmModelMetadata(displayName = "Axon Body 3 Dock - 8 Bay", alias = Nil, serialPrefix = AxonBody3Dock8Bay.masks)

    if (config.getBoolean("adm.allowTaser8Models")) {
      supportedModels += Taser10Handle.toString -> AdmModelMetadata(displayName = "TASER 10", alias = Nil, serialPrefix = Taser10Handle.masks)
      supportedModels += Taser10HandleVr.toString -> AdmModelMetadata(displayName = "TASER 10 VR", alias = Nil, serialPrefix = Taser10HandleVr.masks)
      supportedModels += Taser10CartridgeDuty.toString -> AdmModelMetadata(displayName = "TASER 10 Duty Cartridge", alias = Nil, serialPrefix = Taser10CartridgeDuty.masks)
      supportedModels += Taser10CartridgeHalt.toString -> AdmModelMetadata(displayName = "TASER 10 Halt Cartridge", alias = Nil, serialPrefix = Taser10CartridgeHalt.masks)
      supportedModels += Taser10MagazineDuty.toString -> AdmModelMetadata(displayName = "TASER 10 Duty Magazine", alias = Nil, serialPrefix = Taser10MagazineDuty.masks)
      supportedModels += Taser10MagazineHalt.toString -> AdmModelMetadata(displayName = "TASER 10 Halt Magazine", alias = Nil, serialPrefix = Taser10MagazineHalt.masks)
      supportedModels += Taser10MagazineInert.toString -> AdmModelMetadata(displayName = "TASER 10 Inert Magazine", alias = Nil, serialPrefix = Taser10MagazineInert.masks)
      supportedModels += Taser10MagazineTraining.toString -> AdmModelMetadata(displayName = "TASER 10 Training Magazine", alias = Nil, serialPrefix = Taser10MagazineTraining.masks)
      supportedModels += Taser10MagazineVr.toString -> AdmModelMetadata(displayName = "TASER 10 VR Magazine", alias = Nil, serialPrefix = Taser10MagazineVr.masks)
    }

    supportedModels.toMap
  }

  private val allowedDockBasedRegisterStatus: Seq[DeviceStatus2] = Seq(DeviceStatus2.Assigned, DeviceStatus2.InStock, DeviceStatus2.InEvidence)

  private def isFleetHeathrowTokenScopeList(scopes: Set[String]): Boolean = {
    scopes.contains(ECOMScopes.DEVICE_SNF_ANY_READ) && scopes.contains(ECOMScopes.DEVICE_ANY_READ)
  }

  def addCanaryDevice(maybeAgencyId: String, id: String): Action[AnyContent] = {
    val func = "addCanaryDevice"
    authenticateViaToken.async { authenticatedRequest =>
      logger.debug(func)(DeviceConstants.AgencyIdString -> maybeAgencyId, DeviceConstants.DeviceIdString -> id)

      Convert.tryToUuid(maybeAgencyId).map { agencyId =>
        if (hasDeviceModifyPermissionAgency(agencyId, authenticatedRequest)) {
          val addCanaryRequest = AddCanaryDeviceRequest(partnerId = agencyId.toString, deviceId = id, updatedByFromJwt(authenticatedRequest.jwt))

          val devicePromise: Future[Either[ServiceErrorCode, GetDeviceResponse]] = arsenal.addCanaryDevice(addCanaryRequest)

          def getDeviceWithUser(cds: Either[ServiceErrorCode, GetDeviceResponse]) = cds match {
            case Left(error) => Future(arsenalErrorToResponse(error))
            case Right(response: GetDeviceResponse) => response.device match {
              case None => Future(errorResponse(Results.InternalServerError))
              case Some(device: DeviceMetaData) =>
                val komradeTid = device.owner.map(owner => Seq(toKomradeEntityDescriptor(owner))).getOrElse(Seq())
                toCanaryDevicesWithUsers(Seq(device), komradeTid)
            }
          }

          for {
            deviceResp <- devicePromise
            canaryDevice <- getDeviceWithUser(deviceResp)
          } yield canaryDevice
        } else {
          Future.successful(errorResponse(Results.Forbidden))
        }
      }.getOrElse(Future.successful(BadRequest))
    }
  }

  def assignDeviceHome(agencyId: String, deviceHomeId: String): Action[JsValue] = {
    val func = "assignDeviceHome"
    logger.debug(func)(DeviceConstants.FuncString -> func, DeviceConstants.AgencyIdString -> agencyId)

    authenticateViaToken.async(parse.tolerantJson) { authenticatedRequest =>
      val result =
        for {
          deviceHomeUuid <- FutureEither.successful(validateDeviceHomeId(deviceHomeId))
          response <- setDeviceHomeImpl(agencyId, deviceHomeId = Some(deviceHomeUuid), authenticatedRequest = authenticatedRequest)
        } yield {
          response
        }

      result.fold(l => l, r => r)
    }
  }

  def certificateHandshake(): Action[String] = Action(parse.text).async { implicit req =>
    SelfService.withJwt(arkham, req) { jwt =>
      val domain: String = jwt.domain

      val response = for {
        _ <- komrade.getPartnerIdFromDomain(domain)
        certInfo <- FutureEither(arkham.getCertificate(X509Type.Intermediate).map(e => e.left.map(sec =>
          errorResponse(Results.InternalServerError, detail = Some(sec.name), serviceErrorCode = Some(sec.value)))
        ))
        cert <- FutureEither.successful(certInfo.certificate.toRight(errorResponse(Results.NotFound, detail = Some("no intermediate certificate found"))))
        jwtResponse = {
          val header = Json.obj("alg" -> "ES256", "typ" -> "JWT", "x5c" -> List(cert))
          val payload = Json.obj("dmn" -> domain, "dsn" -> jwt.serialNumber.value, "iat" -> Instant.now().getEpochSecond)

          s"${rawUrlEncode(header)}.${rawUrlEncode(payload)}"
        }
        signJwtResponse <- FutureEither {
          arkham.signDevicePayload(SignRequest(ByteBuffer.wrap(jwtResponse.getBytes))).map(e => e.left.map(sec =>
            errorResponse(Results.InternalServerError, detail = Some(sec.name), serviceErrorCode = Some(sec.value))
          ))
        }
        signature <- FutureEither.successful(
          signJwtResponse.signature.toRight(errorResponse(Results.NotFound, detail = Some("no signature returned when signing jwt")))
        )
      } yield {
        s"$jwtResponse.${rawUrlEncode(signature.toArray)}"
      }
      response.fold(identity, jwt => Ok(Json.obj(dataField -> jwt)))
    }
  }

  def certificates(): Action[String] = Action(parse.text).async { implicit req =>
    val func = "certificates"
    logger.debug(func)()
    arkham.getSigningCertificateChain() map {
      _.fold(_ => Results.InternalServerError, certChain => Ok(certChain.certificateChain.mkString))
    }
  }

  def getSigningCertificates(): Action[String] = Action(parse.text).async { implicit req =>
    val func = "getSigningCertificates"
    logger.debug(func)()
    getCertificateChain(X509Purpose.InternalUse)
  }

  def getEncryptionCertificates(): Action[String] = Action(parse.text).async { implicit req =>
    val func = "getEncryptionCertificates"
    logger.debug(func)()
    getCertificateChain(X509Purpose.InternalAB3PasswordEncryptionUse)
  }

  private def getCertificateChain(x509Purpose: X509Purpose): Future[Result] = {
    arkham.getCertificateChain(x509Purpose).map {
      _.fold(_ => Results.InternalServerError, certChain => Ok(certChain.certificateChain.mkString))
    }
  }

  def decryptSshPassword(deviceModel: String): Action[String] = {
    val func = "decryptSshPassword"
    statsd.incrementCounter("decrypt_ssh_password")

    authenticateViaToken.async(parse.tolerantText) { authenticatedRequest =>
      // Get client ip address for audit purposes
      val clientIpAddress = RequestUtils.getClientIpAddress(authenticatedRequest).getOrElse("")

      // Record decryption attempt
      val userTid = tidFromJwt(authenticatedRequest.jwt)
      val decryptionStartedEvent = UserSshPasswordDecryptionStarted(userTid, userTid, clientIpAddress)
      logAuditEventResult(
        audit.recordEventEndWithSuccess(decryptionStartedEvent.eventId, userTid, userTid, decryptionStartedEvent.toJson),
        UserSshPasswordDecryptionStarted.toString,
        userTid)

      // Log userInfo for each decryption attempt
      logDecryptSshPassword(userTid, deviceModel)

      if (hasDeviceListPermissionAgency(Constants.rootPartnerUuid, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]])) {
        deviceModel match {
          case `AxonBody3Model` => decryptAb3SshPassword(authenticatedRequest, userTid, clientIpAddress)
          case `AxonDockModel` => decryptDockSshPassword(authenticatedRequest, userTid, clientIpAddress)
          case _ =>
            // Record decryption failure
            recordDecryptionFailureAuditEvent(audit, userTid, clientIpAddress)

            logger.error(s"$func: Device model invalid.")("model" -> deviceModel)
            Future.successful(Results.BadRequest)
        }
      }
      else {
        // Record decryption failure
        recordDecryptionFailureAuditEvent(audit, userTid, clientIpAddress)

        logger.error(s"$func: Forbidden")("model" -> deviceModel)
        Future.successful(Results.Forbidden)
      }
    }
  }

  // This function name will be used by infosec to run reports on splunk and security scans.
  // If this name is changed, the developer needs to inform the correct stakeholders in infosec about this change.
  private def logDecryptSshPassword(userTid: AuditTid, deviceModel: String) = {
    val func = "logDecryptSshPassword"

    val agencyId = userTid.domain.getOrElse("")
    val userId = userTid.id.getOrElse("")

    def getUser(): Future[User] = {
      if (Seq(agencyId, userId).forall(id => !StringUtils.isNullOrWhiteSpace(id))) {
        for {
          userResponse <- komrade.getUser(agencyId, userId)
        } yield {
          userResponse.fold(
            err => {
              logger.warn(s"$func: can not get user information from Komrade.")("agencyId" -> agencyId, "userId" -> userId, "deviceModel" -> deviceModel)
              User()
            },
            identity
          )
        }
      } else {
        Future(User())
      }
    }

    getUser().onComplete {
      case Success(user) => logger.info(func)("userName" -> user.username, "lastName" -> user.lastName, "firstName" -> user.firstName, "UUID" -> user.id, "email" -> user.email, "deviceModel" -> deviceModel)
    }
  }

  private def decryptAb3SshPassword(authenticatedRequest: JWTAuthenticatedRequest[String], userTid: com.evidence.service.audit.Tid, clientIpAddress: String) = {
    val base64Token = authenticatedRequest.request.body
    val requestOpt = parseAb3PasswordToken(base64Token)

    val response = for {
      request <- FutureEither.successful(requestOpt.toRight(errorResponse(Results.BadRequest, detail = Some("Failed to parse post body"))))
      response <- FutureEither(arkham.decryptSshPasswordPayload(request).map(maybeMapResponseToErrorResponse))
    } yield response

    response.mapLeft { l =>
      // Record decryption failure
      logger.warn("Unable to decrypt AB3 ssh password")("token" -> authenticatedRequest.request.body)
      recordDecryptionFailureAuditEvent(audit, userTid, clientIpAddress)
      l
    }.map { r =>
      decryptionSuccessHandler(r.decryptedPayloadBase64, audit, userTid, clientIpAddress)
    }.fold(identity, identity)
  }

  private def decryptDockSshPassword(authenticatedRequest: JWTAuthenticatedRequest[String], userTid: com.evidence.service.audit.Tid, clientIpAddress: String) = {
    val base64Token = cleanDockToken(authenticatedRequest.request.body)

    val response = for {
      tokenFields <- FutureEither.successful(parseAb2DockTokenFields(base64Token).toRight(errorResponse(Results.BadRequest, detail = Some("Failed to parse post body"))))

      // Rsa decrypt
      rsaDecryptResponse <- FutureEither(arkham.rsaDecrypt(tokenFields.keyId, tokenFields.sessionKey).map(maybeMapResponseToErrorResponse))
      aesFields <- FutureEither.successful(parseDockAesFields(rsaDecryptResponse).toRight(errorResponse(Results.BadRequest, detail = Some("Failed to parse decrypted aes payload"))))

      // Aes decrypt
      request = AesCbcDecryptRequest(Convert.base64EncodeToString(aesFields.key), Convert.base64EncodeToString(aesFields.initVector), Convert.base64EncodeToString(tokenFields.payload), AesCbcPadding.PKCS5Padding)
      aesDecryptResponse <- FutureEither(arkham.aesCbcDecrypt(request).map(maybeMapResponseToErrorResponse))
    } yield aesDecryptResponse

    response.mapLeft { l =>
      // Record decryption failure
      logger.warn("Unable to decrypt dock ssh password")("token" -> authenticatedRequest.request.body)
      recordDecryptionFailureAuditEvent(audit, userTid, clientIpAddress)
      l
    }.map { r =>
      decryptionSuccessHandler(r.decryptedTextBase64, audit, userTid, clientIpAddress)
    }.fold(identity, identity)
  }

  def create(maybeAgencyId: String): Action[JsValue] = {
    val func = "create"
    authenticateViaToken.async(parse.tolerantJson) { authenticatedRequest =>
      Convert.tryToUuid(maybeAgencyId).map { agencyId =>
        logger.debug(func)()
        if (agencyMatchesAuthenticatedUserAgency(agencyId.toString, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]])) {
          val json = authenticatedRequest.request.body
          val registerRequest = constructRegisterRequest(agencyId.toString, json, authenticatedRequest)
          registerRequest.map { request =>
            (for {
              _ <- FutureEither.successful(validatePermissionToRegisterDevice(agencyId, authenticatedRequest, Some(DeviceMetaData(owner = request.owner, model = Some(request.model)))))
              _ <- FutureEither(validateUserExistsIfOwner(agencyId, request.owner))
              device <- FutureEither(registerDevice(authenticatedRequest, request))
            } yield Created(Json.obj(dataField -> deviceInfoToJson(device))))
            .fold(l => l, r => r)
          }.getOrElse(Future.successful(errorResponse(Results.BadRequest)))
        } else {
          Future.successful(errorResponse(Results.Forbidden))
        }
      }.getOrElse(Future.successful(BadRequest))
    }
  }

  def assignDeviceToSubscriber(maybeAgencyId: String, maybeDeviceId: String, maybeSubscriberId: String): Action[AnyContent] = {
    def assignToSubscriber(deviceId: String, updatedBy: EntityDescriptor): Future[Either[Result, AssignDeviceResponse]] = {
      val owner = EntityDescriptor(TidEntities.Subscriber, maybeSubscriberId, maybeAgencyId)
      val assignRequest = AssignDeviceRequest(maybeAgencyId, deviceId, owner, updatedBy)
      arsenal.assignDevice(assignRequest).map {
        case Left(err) => Left(arsenalErrorToResponse(err))
        case Right(resp) => Right(resp)
      }
    }

    def validateModelBasedModifyPermission(device: Option[DeviceMetaData], authReq: JWTAuthenticatedRequest[AnyContent]) = {
      device.map { metadata =>
        metadata.model match {
          case Some(model) =>
            val hasPermission = {
              if (DeviceConstants.CewDevicesModelPrefix.exists(model.startsWith)) {
                Permissions(Scope(ECOMScopes.CEW_ANY_MODIFY)).validate(authReq.jwt, Map.empty)
              } else if (DeviceConstants.AirDevicesModelPrefix.exists(model.startsWith)) {
                Permissions(Scope(ECOMScopes.AIR_DEVICE_ANY_MODIFY)).validate(authReq.jwt, Map.empty) ||
                  Permissions(Scope(ECOMScopes.DEVICE_ANY_MODIFY)).validate(authReq.jwt, Map.empty)
              } else {
                Permissions(Scope(ECOMScopes.DEVICE_ANY_MODIFY)).validate(authReq.jwt, Map.empty)
              }
            }
            if (hasPermission) Right(()) else Left(errorResponse(Results.Forbidden))
          case None => Left(errorResponse(Results.Forbidden))
        }
      }.getOrElse(Left(errorResponse(Results.NotFound, Some(DeviceConstants.DeviceNotFoundErrorMessage))))
    }

    val func = "assignDeviceToSubscriber"
    authenticateViaToken.async { implicit request =>
      logger.debug(func)(DeviceConstants.AgencyIdString -> maybeAgencyId,
        DeviceConstants.DeviceIdString -> maybeDeviceId,
        DeviceConstants.OwnerIdString -> maybeSubscriberId
      )

      Convert.tryToUuid(maybeAgencyId).map { _ =>
        if (hasRootAgencyAccessPermission(request)) {
          val updatedBy = updatedByFromJwt(request.jwt)
          val result = for {
            deviceId <- FutureEither.successful(validateDeviceId(maybeDeviceId))
            _ <- FutureEither.successful(validateSubscriberId(maybeSubscriberId))
            deviceResponse <- FutureEither(getDeviceByIdWrapper(maybeAgencyId, deviceId.toString))
            _ <- FutureEither.successful(validateModelBasedModifyPermission(deviceResponse.device, request))
            assignResponse <- FutureEither(assignToSubscriber(deviceId.toString, updatedBy))
          } yield {
            Ok(Json.obj(dataField -> deviceInfoToJson(assignResponse.device.get)))
          }
          result.fold(identity, identity)
        } else {
          Future.successful(errorResponse(Results.Forbidden)) 
        }
      }.getOrElse(Future.successful(errorResponse(Results.BadRequest)))
    }
  }

  def bulkTransferDevices(): Action[JsValue] = {
    authenticateViaToken.async(parse.tolerantJson) { authenticatedRequest =>
      val authRequestAsAnyContent = authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]]
      if (hasRootAgencyAccessPermission(authRequestAsAnyContent) && hasDeviceBulkTransferPermissions(authRequestAsAnyContent)) {

        val request = parseBulkDeviceTransferRequest(authenticatedRequest.request.body)
        val currentAgency = request.currentAgency.flatMap(Convert.tryToUuid)
        val targetAgency = request.targetAgency.flatMap(Convert.tryToUuid)
        val model = request.model
        val deviceIds = request.deviceIds

        // Make sure to/from agencies are not the same and we have deviceIds
        def validRequest(fromAgency: UUID, toAgency: UUID): Boolean = {
          fromAgency != toAgency && deviceIds.nonEmpty
        }
        logger.info("bulkTransferDevices")("request" -> request, "currentAgency" -> currentAgency, "targetAgency" -> targetAgency, "model" -> model, "deviceIds" -> deviceIds)

        (currentAgency, targetAgency, model) match {
          case (Some(fromAgency), Some(toAgency), Some(requestedModel)) if (validRequest(fromAgency, toAgency)) =>
            val updatedBy = EntityDescriptor(entityType = TidEntities.Subscriber, id = authenticatedRequest.jwt.subjectId, partnerId = authenticatedRequest.jwt.audienceId)
            val request = BulkDeviceTransferRequest(fromAgency.toString, toAgency.toString, requestedModel, updatedBy, deviceIds)
            arsenal.edcaBulkDeviceTransfer(request).map {
              case Left(err) => arsenalErrorToResponse(err)
              case Right(resp) => Ok(Json.obj("successfulTransfers" -> resp.successfulTransferCount))
            }
          case _ =>
            Future.successful(errorResponse(Results.BadRequest))
        }
      } else {
        Future.successful(errorResponse(Results.Forbidden))
      }
    }
  }

  def bulkAssignDevices(maybeAgencyId: String): Action[JsValue] = {
    val func = "bulkAssignDevices"
    authenticateViaToken.async(parse.tolerantJson) { authenticatedRequest =>
      val updatedBy = updatedByFromJwt(authenticatedRequest.jwt)

      val res = for {
        requestParsed <- FutureEither.successful(validJsOrBadRequest(authenticatedRequest.request.body.validate[BulkDeviceAssignRequest]))
        agencyId <- FutureEither.successful(UUIDHelper.validateUuid(maybeAgencyId, func, s"agencyId '$maybeAgencyId' invalid'"))
        deviceAssignment = requestParsed.deviceAssignments.map(da => BulkDeviceAssignment(da.deviceIds,  EntityDescriptor(TidEntities.Subscriber, da.owner, da.owner)))
        request = BulkAssignDeviceRequest(deviceAssignment = deviceAssignment, partnerId = agencyId.toString, updatedBy = updatedBy)
        resp <- FutureEither(arsenal.bulkAssignDevice(request).map {
          case Left(err) => Left(arsenalErrorToResponse(err))
          case Right(resp) => Right(resp)}
        )
      } yield (resp, deviceAssignment)

      res.fold(l => l, r => if(bulkDeviceAssignmentLimitCheck(r._2, BulkDeviceAssignLimit) || (r._1.errors.size == r._2.flatMap(x => x.deviceIds).size)) {
            BadRequest(Json.toJson(r._1))
          } else if (r._1.errors.isEmpty) {
            Ok(Json.toJson(r._1))
          } else {
            PartialContent(Json.toJson(r._1))
        }
      )
    }
  }

  def bulkDeviceAssignmentLimitCheck(bulkDeviceAssignmentList: List[BulkDeviceAssignment], limit: Int): Boolean = {
    bulkDeviceAssignmentList.flatMap(x => x.deviceIds).size > limit
  }

  def downloadFirmware(version: String, fileName: Option[String] = None): Action[AnyContent] = {
    val func = "downloadFirmware"
    authenticateViaToken.async { implicit request =>
      val partnerId = request.jwt.audienceId
      val deviceId = request.jwt.subjectId

      val result = for {
        response <- FutureEither(arsenal.getFirmwareDownload(GetFirmwareDownloadRequest(partnerId, deviceId, version, fileName)))
          .mapLeft(sec => arsenalErrorToResponse(sec))
        transformedUrl <- transformUrl(func, partnerId, Some(deviceId), Some(version), response, None)
      } yield transformedUrl

      result.fold(identity, url => Redirect(url, 302))
    }
  }

  def downloadFirmwareArtifact(version: String, filename: String): Action[AnyContent] = {
    downloadFirmware(version, Option(filename))
  }

  def downloadLog(deviceModel: String, serial: String, fileKey: String, timestamp: String): Action[AnyContent] = {
    val func = "downloadLog"
    authenticateViaToken.async { implicit ar =>
      val (permissions, params) = getDeviceEDCAPermission
      if (permissions.validate(ar.jwt, params)) {

        val filename = s"$deviceModel-$serial-$timestamp"
        val cleanFilename = filename.replaceAllLiterally(".", "-").replaceAllLiterally(":", "-")

        val preassignedUrlReq = GetDeviceLogUrlRequest(fileKey)
        val urlPromise = arsenal.getDeviceLogUrl(preassignedUrlReq)
        urlPromise.flatMap { resp =>
          resp.fold(
            l => Future.successful(arsenalErrorToResponse(l)),
            r => {
              logger.info(func)(DeviceConstants.RequestString -> preassignedUrlReq, "response" -> r.url)
              RequestUtils.streamFile(wsClient, StreamFileRequest.streamDefaultFromRequest(r.url, Some(cleanFilename), ar.request))
            })
        }
      }
      else {
        Future.successful(errorResponse(Results.Forbidden))
      }
    }
  }

  def getAdminCommands(model: String): Action[AnyContent] = {
    val func = "getAdminCommands"
    logger.debug(func)(DeviceConstants.ModelString -> model)

    authenticateViaToken.async { authenticatedRequest =>
      if (hasRootDeviceExecutePermission(authenticatedRequest)) {
        adminCommandsConfig.get(model).map { commands =>
          Future.successful(Ok(
            Json.obj(
              dataField -> Json.obj(
                "adminCommands" -> commands,
                "adminCommandMaxBytes" -> adminCommandMaxBytes,
              )
            )
          ))
        }.getOrElse(Future.successful(errorResponse(Results.BadRequest)))
      } else {
        Future.successful(errorResponse(Results.Forbidden))
      }
    }
  }

  def getAuditDocument(maybeAgencyId: String, id: String, includeConfidentialActivity: Option[Boolean], language: Option[String]): Action[AnyContent] = {
    val func = "getAuditDocument"
    logger.debug(func)(DeviceConstants.AgencyIdString -> maybeAgencyId, DeviceConstants.IdString -> id)

    authenticateViaToken.async { authenticatedRequest =>
      Convert.tryToUuid(maybeAgencyId).map { agencyId =>

        val getDeviceRequest = GetDeviceByIdQuery(agencyId.toString, id)
        val promise = arsenal.getDeviceById(getDeviceRequest)

        val requestorTid: Tid = Tid(
          Some(authenticatedRequest.jwt.subjectType),
          Some(authenticatedRequest.jwt.subjectId),
          authenticatedRequest.jwt.subjectDomain)

        def dispatchReportRequest(deviceResp: Either[Result, GetDeviceResponse]): Future[Either[Result, String]] = {
          deviceResp match {
            case Left(error) => Future(Left(error))
            case Right(response) =>
              response.device.map(device => {
                val (permissions, params) = getDeviceReadPermission(agencyId, device)
                if (permissions.validate(authenticatedRequest.jwt, params)) {
                  val fileFormatOpt = ReportFileFormat.valueOf(authenticatedRequest.getQueryString("fileFormat").getOrElse(""))
                  fileFormatOpt match {
                    case None => Future.successful(Left(BadRequest))
                    case Some(fileFormat) =>
                      val deviceTidParams = Map(
                        "forTidId" -> Seq(id),
                        "forTidEnt" -> Seq(TidEntities.Device.toString),
                        "forTidDomain" -> Seq(agencyId.toString))

                      val queryOrHeaderLang = if (language.exists(_.trim.nonEmpty)) Seq(language.get) else authenticatedRequest.acceptLanguages.map(_.code)
                      val lang = Map("language" -> queryOrHeaderLang)
                      val datesMap = getAuditDates(authenticatedRequest.getQueryString(DeviceConstants.StartDateString), authenticatedRequest.getQueryString(DeviceConstants.EndDateString))
                      datesMap match {
                        case Left(result) => Future(Left(result))
                        case Right(dates) =>
                          val parameters = authenticatedRequest.queryString ++ deviceTidParams ++ lang ++ dates
                          val reportConfidentialOptFuture = AuditUtils.getReportConfidentialOptions(komrade, agencyId, includeConfidentialActivity, authenticatedRequest)
                          val reportConfidentialOpt = reportConfidentialOptFuture.map {
                            case Left(error) =>
                              logger.error(func)("message" -> error.toString())
                              Left(Results.InternalServerError)
                            case Right(reportConfidentialOpt) =>
                              Right(reportConfidentialOpt)
                          }
                          val res = for {
                            rco <- FutureEither(reportConfidentialOpt)
                            generateReportRequest = GenerateReportRequest(agencyId.toString, requestorTid, ReportType.DeviceAuditTrail, parameters, fileFormat, Some(rco))
                            result <- FutureEither(reports.createReport2(generateReportRequest, requestorTid))
                          } yield result

                          res.future

                      }
                  }
                } else {
                  Future.successful(Left(Results.Forbidden))
                }
              }).getOrElse(Future.successful(Left(Results.InternalServerError)))
          }
        }

        def getReport(reportId: Either[Result, String]): Future[Result] = {
          reportId match {
            case Left(errorResult) => Future(errorResult)
            case Right(reportId) =>
              Convert.tryToUuid(reportId).map { reportUUID =>
                reports.pollForReport(agencyId, reportUUID, requestorTid)
              }.getOrElse(Future.successful(BadRequest))
          }
        }

        for {
          deviceResp <- promise
          filteredResp <- filterDeviceModels(deviceResp)
          reportId <- dispatchReportRequest(filteredResp)
          deviceReport <- getReport(reportId)
        } yield {
          deviceReport
        }
      }.getOrElse(Future.successful(BadRequest))
    }
  }

  def getDeviceAuditTrailJson(maybeAgencyId: String, id: String, includeConfidentialActivity: Option[Boolean], language: Option[String]): Action[AnyContent] = {
    val func = "getDeviceAuditTrailJson"
    logger.debug(func)(DeviceConstants.AgencyIdString -> maybeAgencyId, DeviceConstants.IdString -> id)
    authenticateViaToken.async { authenticatedRequest =>
      Convert.tryToUuid(maybeAgencyId).map { agencyId =>

        val getDeviceRequest = GetDeviceByIdQuery(agencyId.toString, id)
        val promise = arsenal.getDeviceById(getDeviceRequest)

        val requestorTid: Tid = Tid(
          Some(authenticatedRequest.jwt.subjectType),
          Some(authenticatedRequest.jwt.subjectId),
          authenticatedRequest.jwt.subjectDomain)

        def getJSON(deviceResp: Either[Result, GetDeviceResponse]): Future[Result] = {
          deviceResp match {
            case Left(error) => Future.successful(error)
            case Right(response) =>
              response.device.map(device => {
                val (permissions, params) = getDeviceReadPermission(agencyId, device)
                if (permissions.validate(authenticatedRequest.jwt, params)) {
                  val idParams = Map(DeviceConstants.DeviceIdString -> Seq(device.id.get), "subscriberId" -> Seq(requestorTid.id.get))
                  val queryOrHeaderLang = if (language.exists(_.trim.nonEmpty)) Seq(language.get) else authenticatedRequest.acceptLanguages.map(_.code)
                  val lang = Map("language" -> queryOrHeaderLang)
                  val datesMap = getAuditDates(authenticatedRequest.getQueryString(DeviceConstants.StartDateString), authenticatedRequest.getQueryString(DeviceConstants.EndDateString))
                  datesMap match {
                    case Left(result) => Future.successful(result)
                    case Right(dates) =>
                      val parameters = authenticatedRequest.queryString ++ idParams ++ lang ++ dates

                      val reportConfidentialOpt = AuditUtils.getReportConfidentialOptionsAsMap(komrade, agencyId, includeConfidentialActivity, authenticatedRequest)
                      reportConfidentialOpt.flatMap(r => r match {
                        case Left(error) =>
                          logger.error(func)("message" -> error.toString())
                          Future(Results.InternalServerError)
                        case Right(rco) =>
                          val paramsWithConfidentialOptions = parameters ++ rco
                          reports.getDeviceAuditJson(agencyId.toString, ReportType.DeviceAuditTrail, paramsWithConfidentialOptions, requestorTid)
                      })
                  }
                } else {
                  Future.successful(Results.Forbidden)
                }
              }).getOrElse(Future.successful(Results.InternalServerError))
          }
        }

        for {
          deviceResp <- promise
          filteredResp <- filterDeviceModels(deviceResp)
          data <- getJSON(filteredResp)
        } yield {
          data
        }
      }.getOrElse(Future.successful(BadRequest))
    }
  }

  // The following endpoint is documented in our Partner API guide.
  // Tread with extra care when touching it.
  def getDeviceById(maybeAgencyId: String, id: String): Action[AnyContent] = {
    val func = "getDeviceById"
    logger.debug(func)(DeviceConstants.AgencyIdString -> maybeAgencyId, DeviceConstants.IdString -> id)

    Convert.tryToUuid(id) match {
      case None => Action(errorResponse(resultStatus = Results.BadRequest, detail = Some("Invalid deviceId")))
      case Some(deviceId) => getDeviceImpl(maybeAgencyId, GetDeviceById(deviceId.toString))
    }
  }

  def getDeviceBySerial(maybeAgencyId: String, serial: String): Action[AnyContent] = {
    val func = "getDeviceBySerial"
    logger.debug(func)(DeviceConstants.AgencyIdString -> maybeAgencyId, DeviceConstants.SerialString -> serial)

    getDeviceImpl(maybeAgencyId, GetDeviceBySerial(serial))
  }

  def deviceTargetFirmwareVersion(): Action[AnyContent] = {
    val func = "getDeviceTargetFirmwareVersion"

    authenticateViaToken.async { authenticatedRequest =>
      val maybeDeviceId = authenticatedRequest.jwt.subjectId
      val maybeAgencyId = authenticatedRequest.jwt.audienceId

      logger.debug(func)(DeviceConstants.AgencyIdString -> maybeAgencyId, DeviceConstants.DeviceIdString -> maybeDeviceId)

      Convert.tryToUuid(maybeDeviceId).map { deviceId =>
        val result = for {
          agencyId <- FutureEither.successful(validateAgencyId(maybeAgencyId))
          successJson <- FutureEither(getDeviceTargetFirmwareVersion(agencyId, deviceId))
        } yield {
          Ok(successJson)
        }
        result.fold(l => l, r => r)
      }.getOrElse(Future.successful(errorResponse(Results.BadRequest, detail = Some("Invalid deviceId"))))
    }
  }

  def partnerLatestFirmwareVersion(agencyId: String, deviceModel: String, artifactType: Option[String], subType: Option[String]): Action[AnyContent] = {
    val func = "partnerLatestFirmwareVersion"

    statsDTimed(func, true) {
      security.authorize(noPermissions(agencyId)).async { implicit authenticatedRequest =>
        logger.debug(func)(
          DeviceConstants.AgencyIdString -> agencyId,
          DeviceConstants.DeviceModelString -> deviceModel,
          DeviceConstants.ArtifactTypeString -> artifactType.getOrElse(""),
          DeviceConstants.SubTypeString -> subType.getOrElse(""))

        val result = for {
          validatedAgencyId <- FutureEither.successful(validateAgencyId(agencyId))
          successJson <- FutureEither(getPartnerLatestFirmwareVersion(validatedAgencyId, deviceModel, artifactType, subType))
        } yield {
          Ok(successJson)
        }
        result.fold(l => l, r => r)
      }
    }
  }

  def partnerLatestFirmware(agencyId: String, deviceModel: String, artifactType: Option[String], subType: Option[String], redirectToDownload: Option[Boolean]): Action[AnyContent] = {
    val func = "partnerLatestFirmware"

    statsDTimed(func, true) {
      security.authorize(noPermissions(agencyId)).async { implicit authenticatedRequest =>
        logger.debug(func)(
          DeviceConstants.AgencyIdString -> agencyId,
          DeviceConstants.DeviceModelString -> deviceModel,
          DeviceConstants.ArtifactTypeString -> artifactType.getOrElse(""),
          DeviceConstants.SubTypeString -> subType.getOrElse(""),
          DeviceConstants.RedirectToDownloadString -> redirectToDownload.getOrElse(false))

        val serverError = errorResponse(Results.InternalServerError)

        val result = for {
          validatedAgencyId <- FutureEither.successful(validateAgencyId(agencyId))
          successJson <- FutureEither(getPartnerLatestFirmware(validatedAgencyId, deviceModel, artifactType, subType, redirectToDownload))
        } yield {
          if (redirectToDownload.getOrElse(false)) {
            val downloadUrl =(successJson \ "data" \ "downloadUrl").as[String]
            Redirect(downloadUrl, 302)
          } else {
            Ok(successJson)
          }
        }
        result.fold(l => l, r => r)
      }
    }
  }

  def getFirmwareInfo(
    agencyId: String,
    models: String,
    status2: Option[String]
  ): Action[AnyContent] = {
    val func = "getFirmwareInfo"

    logger.debug(func)(
      DeviceConstants.FuncString -> func,
      DeviceConstants.AgencyIdString -> agencyId,
      DeviceConstants.ModelString -> models
    )

    authenticateViaToken.async { authenticatedRequest =>
      val result =
        for {
          agencyUuid <- FutureEither.successful(validateAgencyId(agencyId))
          modelGrouping <- FutureEither.successful(ModelGroupingUtils.validateModelGroups(models))
          baseModel <- FutureEither.successful(Right(modelGrouping.baseModel))
          allowedModels <- FutureEither.successful(Right(modelGrouping.allowedModels))
          modelsToSearch <- FutureEither.successful(Right(allowedModels.mkString(DeviceConstants.CommaChar.toString)))
          _ <- FutureEither.successful(hasPermissionsToGetDashboardInfo(baseModel, authenticatedRequest, agencyUuid, "Requesting entity does not have permissions to get firmware info"))
          currentFirmwareVersionResponse <- FutureEither(getCurrentFirmwareVersion(agencyUuid, baseModel))
          agencyFirmwareVersionsResponse <- FutureEither(getAgencyFirmwareVersions(agencyUuid, modelsToSearch, status2))
          successJson <- FutureEither.successful {
            Right(
              agencyFirmwareInfoToJson(
                partnerId = agencyUuid,
                model = modelsToSearch,
                models = allowedModels,
                baseModel = baseModel,
                agencyFirmwareVersionsResponse = agencyFirmwareVersionsResponse,
                currentFirmwareVersionResponse = currentFirmwareVersionResponse
              )
            )
          }
        } yield {
          Ok(successJson)
        }

      result.fold(l => l, r => r)
    }
  }

  /**
   * Get functional test statistics for the given model for the last numberOfDays.
   *
   * @param agencyId
   * @param models - comma separated list of models
   * @param numberOfDays Number of days to pivot the data on. Must be: 0 &lt numberOfDays &le 365
   * @return Result will be functional tests done on last number of days, before those days, and unknown.
   */

  def getFunctionalTestStats(agencyId: String, models: String, numberOfDays: Int, status2: Option[String]): Action[AnyContent] = {
    val func = "getFunctionalTestStats"
    logger.debug(func)(DeviceConstants.AgencyIdString -> agencyId, DeviceConstants.ModelString -> models, "NumberOfDays" -> numberOfDays)

    authenticateViaToken.async { authenticatedRequest =>
      val result =
        for {
          agencyUuid <- FutureEither.successful(validateAgencyId(agencyId))
          modelGrouping <- FutureEither.successful(ModelGroupingUtils.validateModelGroups(models))
          baseModel <- FutureEither.successful(Right(modelGrouping.baseModel))
          allowedModels <- FutureEither.successful(Right(modelGrouping.allowedModels))
          numberOfDaysVerified <- FutureEither.successful(validateNumberRange(numberInput = numberOfDays, minInclusive =  1, maxInclusive = Int.MaxValue))
          _ <- FutureEither.successful(hasPermissionsToGetDashboardInfo(baseModel, authenticatedRequest, agencyUuid, "Requesting entity does not have permissions to get functional test stats"))
          response <- FutureEither(getFunctionalTestStats(partnerId = agencyUuid, models = allowedModels, baseModel = baseModel, numberOfDays = numberOfDaysVerified, status2 = status2))
        } yield {
          Ok(response)
        }
      result.fold(l => l, r => r)
    }
  }

  def listCanaryDevices(maybeAgencyId: String): Action[AnyContent] = {
    val func = "listCanaryDevices"
    authenticateViaToken.async { authenticatedRequest =>
      logger.debug(func)(DeviceConstants.AgencyIdString -> maybeAgencyId)

      Convert.tryToUuid(maybeAgencyId).map { agencyId =>
        if (hasDeviceListPermissionAgency(agencyId, authenticatedRequest)) {
          val fields = Some(Fields(firmware = Some(true)))
          val listCanaryDevicesRequest = ListCanaryDevicesRequest(partnerId = agencyId.toString, fields = fields)

          val devicesPromise = arsenal.listCanaryDevices(listCanaryDevicesRequest)

          def getDevicesWithUsers(cds: Either[ServiceErrorCode, ListCanaryDevicesResponse]) = cds match {
            case Left(error) => Future(arsenalErrorToResponse(error))
            case Right(response: ListCanaryDevicesResponse) => response.devices match {
              case None => Future(getEmptyListResponse(authenticatedRequest.request.uri))
              case Some(deviceList) =>
                val komradeTids = deviceList.flatMap(device => device.owner).map(toKomradeEntityDescriptor)
                toCanaryDevicesWithUsers(deviceList, komradeTids)
            }
          }

          for {
            devicesResp <- devicesPromise
            canaryDevices <- getDevicesWithUsers(devicesResp)
          } yield canaryDevices
        } else {
          Future.successful(errorResponse(Results.Forbidden))
        }
      }.getOrElse(Future.successful(BadRequest))
    }
  }

  def lookupOrRegisterAirIntegratorDevice(agencyId: String, serial: String): Action[JsValue] = {
    val invalidModelResponse = errorResult(Results.Forbidden, "Invalid device model type")
    val forbidRegistrationResponse = errorResult(Results.Forbidden, "Not permitted to register new device")
    val forbidLookupResponse = errorResult(Results.Forbidden, "Not permitted to lookup device")
    val materializer = ActorMaterializer()(ActorSystem())

    def getErrorsFromByteString(byteString: ByteString): Seq[String] = {
      try {
        val responseJs = Json.parse(byteString.utf8String).as[JsObject]
        val errors = (responseJs \ errorsField).as[JsArray]
        errors.value.map(err => (err \ canonicalStringField).as[String]).toSeq
      } catch {
        case _: Throwable => Seq()
      }
    }

    authenticateViaToken.async(parse.tolerantJson) { ar =>
      val json = ar.request.body
      val model = (json \ DeviceConstants.DataString \ DeviceConstants.AttributesString \ DeviceConstants.ModelString).asOpt[String]
      if (!model.contains(Model.AxonAir.entryName)) Future.successful(invalidModelResponse)
      else {
        val getRequest = ar.map(AnyContentAsJson)
        getDeviceBySerial(agencyId, serial)(getRequest).flatMap { getResult =>
          if (getResult.header.status == 404) {
            create(agencyId)(ar).flatMap { createResult =>
              for {
                bytes <- createResult.body.consumeData(materializer)
              } yield {
                val errors = getErrorsFromByteString(bytes)
                // Map generic operation not permitted errors to a more helpful string
                if (errors.contains(OperationNotPermittedCanonicalString)) {
                  forbidRegistrationResponse
                } else {
                  createResult
                }
              }
            }
          } else {
            for {
              bytes <- getResult.body.consumeData(materializer)
            } yield {
              val errors = getErrorsFromByteString(bytes)
              // Map generic operation not permitted errors to a more helpful string
              if (errors.contains(OperationNotPermittedCanonicalString)) {
                forbidLookupResponse
              } else {
                getResult
              }
            }
          }
        }
      }
    }
  }

  /**
    * this endpoint doesn't need any authentication
    */
  def listSupportedModelsForADM(): Action[AnyContent] = {
    Action.async { implicit request =>
      Future.successful(
        Ok(
          Json.obj(
            dataField ->
            supportedAdmModels.map {
              case (model, metadata) =>
                Json.obj(
                  modelField -> model,
                  DeviceConstants.DisplayNameString -> metadata.displayName,
                  DeviceConstants.AliasString -> metadata.alias,
                  DeviceConstants.SerialPrefixString -> metadata.serialPrefix)
            })))
    }
  }

  def initiateEvidenceOffload(maybeAgencyId: String, deviceId: String, evidenceId: String): Action[AnyContent] = {
    sendEvidenceOffloadRequestToConnectedDevice(maybeAgencyId, deviceId, evidenceId, EvidenceOffloadRequestType.Initiate)
  }

  private def sendEvidenceOffloadRequestToConnectedDevice(maybeAgencyId: String, deviceId: String, evidenceId: String, requestType: EvidenceOffloadRequestType): Action[AnyContent] = {
    val func = "sendEvidenceOffloadRequestToConnectedDevice"
    authenticateViaToken.async { authenticatedRequest =>
      logger.debug(func)(
        DeviceConstants.AgencyIdString -> maybeAgencyId,
        DeviceConstants.DeviceIdString -> deviceId,
        DeviceConstants.EvidenceIdString -> evidenceId,
        DeviceConstants.RequestTypeString -> requestType
      )
      val result = for {
        agencyUuid <- FutureEither.successful(Convert.tryToUuid(maybeAgencyId).toRight(errorResponse(Results.BadRequest)))
        _ <- {
          val hasPermission = hasEvidenceOffloadPermission(agencyUuid, authenticatedRequest)
          FutureEither.successful(Either.cond(hasPermission, Unit, errorResponse(Results.Forbidden)))
        }
        json <- FutureEither(sendEvidenceOffloadRequest(agencyUuid.toString, deviceId, evidenceId, requestType))
      } yield {
        Ok(json)
      }
      result.fold(identity, identity)
    }
  }

  def removeCanaryDevice(maybeAgencyId: String, id: String): Action[AnyContent] = {
    val func = "removeCanaryDevice"
    authenticateViaToken.async { authenticatedRequest =>
      logger.debug(func)(DeviceConstants.AgencyIdString -> maybeAgencyId, DeviceConstants.DeviceIdString -> id)

      Convert.tryToUuid(maybeAgencyId).map { agencyId =>
        if (hasDeviceModifyPermissionAgency(agencyId, authenticatedRequest)) {
          val removeCanaryRequest = RemoveCanaryDeviceRequest(partnerId = agencyId.toString, deviceId = id, updatedByFromJwt(authenticatedRequest.jwt))
          val promise = arsenal.removeCanaryDevice(removeCanaryRequest)
          for (resp <- promise) yield {
            resp match {
              case Left(error) => arsenalErrorToResponse(error)
              case Right(_) => NoContent
            }
          }
        } else {
          Future.successful(errorResponse(Results.Forbidden))
        }
      }.getOrElse(Future.successful(BadRequest))
    }
  }

  def removeDeviceHomeFromDevices(agencyId: String): Action[JsValue] = {
    val func = "removeDeviceHomeFromDevices"
    logger.debug(func)(DeviceConstants.FuncString -> func, DeviceConstants.AgencyIdString -> agencyId)

    authenticateViaToken.async(parse.tolerantJson) { authenticatedRequest =>
      val result =
        for {
          response <- setDeviceHomeImpl(agencyId = agencyId, deviceHomeId = None, authenticatedRequest = authenticatedRequest)
        } yield {
          response
        }

      result.fold(l => l, r => r)
    }
  }

  // The following endpoint is documented in our Partner API guide.
  // Tread with extra care when touching it.
  def search(maybeAgencyId: String, maybeOffset: Option[Int], maybeLimit: Option[Int]): Action[AnyContent] = {
    def checkPermission(authenticatedRequest: JWTAuthenticatedRequest[AnyContent], agencyId: UUID): Either[Result, Unit] = {
      val (permissions, params) = getDeviceSearchPermission(agencyId, authenticatedRequest)
      if (permissions.validate(authenticatedRequest.jwt, params)) {
        Right((): Unit)
      } else {
        Left(errorResponse(resultStatus = Results.Forbidden, detail = Some("Requesting entity does not have permission to search devices")))
      }
    }

    def getLimitToOwnerId(authenticatedRequest: JWTAuthenticatedRequest[AnyContent]): Option[String] = {
      // Only API clients get the scope DEVICE_ANY_READ
      val hasFullPerms = (
        (authenticatedRequest.jwt.isSubjectAuthClient() && authenticatedRequest.jwt.scopes.contains(ECOMScopes.DEVICE_ANY_READ))
        || isFleetHeathrowTokenScopeList(authenticatedRequest.jwt.scopes)
        )

      val canSearchAll = authenticatedRequest.jwt.scopes.contains(ECOMScopes.DEVICE_ANY_LIST) || hasFullPerms
      if (canSearchAll) {
        None
      } else {
        Some(authenticatedRequest.jwt.subjectId)
      }
    }

    def searchDevices(authenticatedRequest: JWTAuthenticatedRequest[AnyContent],
                      agencyId: UUID,
                      queryParams: DeviceQueryParams
                     ): Future[Either[Result, JsObject]] = {
      val searchClient = DeviceSearchClient(agencyId, solrClient)
      processDeviceSearch(agencyId, authenticatedRequest, queryParams, searchClient, true)
    }

    authenticateViaToken.async { authenticatedRequest =>
      val result =
        for {
          agencyUuid <- FutureEither.successful(validateAgencyId(maybeAgencyId))
          _ <- FutureEither.successful(checkPermission(authenticatedRequest, agencyUuid))
          limitToOwnerId <- FutureEither.successful(Right(getLimitToOwnerId(authenticatedRequest)))
          queryParams <- FutureEither.successful(getSearchParamsFromQuery(authenticatedRequest, agencyUuid, limitToOwnerId, maybeOffset, maybeLimit))
          json <- FutureEither(searchDevices(authenticatedRequest, agencyUuid, queryParams))
        } yield {
          Ok(json)
        }

      result.fold(l => l, r => r)
    }
  }

  def edcaSearch(maybeAgencyId: String, maybeOffset: Option[Int], maybeLimit: Option[Int]): Action[AnyContent] = {
    def checkPermission(authenticatedRequest: JWTAuthenticatedRequest[AnyContent], agencyId: UUID): Either[Result, Unit] = {
      if (hasRootAgencyAccessPermission(authenticatedRequest) && authenticatedRequest.jwt.scopes.contains(ECOMScopes.DEVICE_ANY_LIST)) {
        Right((): Unit)
      } else {
        Left(errorResponse(resultStatus = Results.Forbidden, detail = Some("Requesting entity does not have permission to search devices")))
      }
    }

    authenticateViaToken.async { authenticatedRequest =>
      val result =
        for {
          agencyUuid <- FutureEither.successful(validateAgencyId(maybeAgencyId))
          _ <- FutureEither.successful(checkPermission(authenticatedRequest, agencyUuid))
          queryParams <- FutureEither.successful(getSearchParamsFromQuery(authenticatedRequest, agencyUuid, None, maybeOffset, maybeLimit))
          json <- FutureEither(processDeviceSearch(agencyUuid, authenticatedRequest, queryParams,
            DeviceSearchClient(agencyUuid, solrClient), false))
        } yield {
          Ok(json)
        }
      result.fold(identity, identity)
    }
  }

  def searchLogs(): Action[AnyContent] = {
    authenticateViaToken.async { authenticatedRequest =>
      val (permissions, params) = getDeviceEDCAPermission
      if (permissions.validate(authenticatedRequest.jwt, params)) {
        val searchType = authenticatedRequest.getQueryString("searchType")
        searchType match {
          case Some(st) =>
            st.toLowerCase match {
              case LogConstants.SerialSearchType => searchLogsByDevice(authenticatedRequest)
              case LogConstants.PartnerSearchType => searchLogsByPartner(authenticatedRequest)
              case LogConstants.FileKeySearchType => searchLogsByFileKey(authenticatedRequest)
              case _ => Future.successful(errorResponse(Results.BadRequest))
            }
          case None => Future.successful(errorResponse(Results.BadRequest))
        }
      } else {
        Future.successful(errorResponse(Results.Forbidden))
      }
    }
  }

  def sendAdminCommands(): Action[JsValue] = {
    val func = "sendAdminCommands"
    logger.debug(func)()

    def badRequest[T](field: String, value: Option[T]): Result = {
      logger.warn(func)("error" -> "badRequest", field -> value)
      errorResponse(Results.BadRequest, detail = Some(s"invalid $field: $value"))
    }

    def getDeviceTargetBySerial(serial: String, partnerId: UUID): FutureEither[Result, Target] = {
      val future = getResponseForGetDeviceRequest(partnerId, GetDeviceBySerial(serial), fields = None).map {
        case Left(error) => Left(arsenalErrorToResponse(error))
        case Right(resp) =>
          val targetOpt = for {
            device <- resp.device
            id <- device.id
          } yield Target.DeviceId(id)
          targetOpt.toRight {
            logger.warn(func)("error" -> "notFound", "serial" -> serial, "partner" -> partnerId)
            errorResponse(Results.NotFound, detail = Some("device not found"))
          }
      }
      FutureEither(future)
    }

    def generateAuditEvents(userTid: AuditTid, action: String, deviceSerial: Option[String],
      partnerId: UUID, idempotencyKey: String, command: String, clientIpAddress: String): (AuditEvent, AuditEvent) = {
      val partnerTid = AuditTid(TidEntities.Partner, Some(partnerId.toString), Some(partnerId.toString))
      val subscriberAudit = SubscriberAdminCommandSent(
        targetTID = userTid,
        updatedByTID = userTid,
        remoteIpAddr = clientIpAddress,
        action = action,
        deviceSerial = deviceSerial,
        partnerTID = partnerTid,
        idempotencyKey = idempotencyKey,
        command = command
      )
      val partnerAudit = PartnerAdminCommandSent(
        targetTID = AuditTid(TidEntities.Partner, Some(partnerId.toString), Some(partnerId.toString)),
        updatedByTID = userTid,
        remoteIpAddr = clientIpAddress,
        action = action,
        deviceSerial = deviceSerial,
        idempotencyKey = idempotencyKey,
        command = command
      )
      (subscriberAudit, partnerAudit)
    }

    def logAuditEventSuccess(events: Seq[AuditEvent], request: EdcaSendAdminCommandsRequest, response: PushAdminCmdResponse, userTid: AuditTid): Unit = {
      events.foreach { event =>
        audit.recordEventEndWithSuccess(event.eventId, event.targetTID, event.updatedByTID, event.toJson)
      }
      logger.info("sendAdminCommandSucceeded")("request" -> request, "response" -> response, "actor" -> userTid)
      statsd.increment("sendAdminCommandSucceeded", s"action:${request.action}", s"model:${request.model}")
    }

    def logAuditEventFailure(events: Seq[AuditEvent], request: EdcaSendAdminCommandsRequest, userTid: AuditTid, logVars: LogVariable*): Unit = {
      events.foreach { event =>
        audit.recordEventEndWithFailure(event.eventId, event.targetTID, event.updatedByTID, event.toJson)
      }
      logger.info("sendAdminCommandFailed")(Seq("request" -> request, "actor" -> userTid) ++ logVars: _*)
      statsd.increment("sendAdminCommandFailed", s"action:${request.action}", s"model:${request.model}")
    }

    authenticateViaToken.async(parse.tolerantJson) { authenticatedRequest =>
      val authRequestAsAnyContent = authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]]
      if (hasRootDeviceExecutePermission(authRequestAsAnyContent)) {
        val request = parseSendAdminCommandsRequest(authenticatedRequest.request.body)
        val idempotencyKey = request.idempotencyKey.getOrElse(UUID.randomUUID().toString)

        val result = for {
          partnerId <- request.partnerId.toRight(badRequest("partnerId", request.partnerId))
          partnerUuid <- Convert.tryToUuid(partnerId).toRight(badRequest("partnerId", request.partnerId))
          _ <- Convert.tryToUuid(idempotencyKey).toRight(badRequest("idempotencyKey", request.idempotencyKey))
          model <- request.model.toRight(badRequest("model", request.model))
          validCommands <- adminCommandsConfig.get(model).toRight(badRequest("model", request.model))
          command <- request.command.filter(_.length <= adminCommandMaxBytes).toRight(badRequest("command", request.command))
          _ <- validCommands.find(regex => command.matches(regex)).toRight(badRequest("command", request.command))
          expirationTime <- request.expirationEpochSeconds.filter(time => time == 0 || time > System.currentTimeMillis()/1000)
            .toRight(badRequest("expirationEpochSeconds", request.expirationEpochSeconds)) // 0 is valid and it means firmware default expiration time
          action <- request.action.toRight(badRequest("action", request.action))
          serial <- action match {
            case "target" => if (request.serial.isDefined) Right(request.serial) else Left(badRequest("serial", request.serial))
            case "broadcast" => Right(None)
          }
        } yield {
          val correlationId = UUID.randomUUID().toString
          val userTid = tidFromJwt(authenticatedRequest.jwt)
          val clientIpAddress = RequestUtils.getClientIpAddress(authenticatedRequest).getOrElse("")

          // start audit
          val (subscriberAudit, partnerAudit) = generateAuditEvents(userTid, action, serial, partnerUuid, idempotencyKey, command, clientIpAddress)
          val auditEvents = Seq(subscriberAudit, partnerAudit)

          // send command
          val requestInfo = RequestInfo.toThrift(RequestInfo.fromExternalCorrelationId(correlationId))
          val resp = for {
            _ <- FutureEither(audit.recordEventStart(subscriberAudit.eventId, subscriberAudit.targetTID, subscriberAudit.updatedByTID, subscriberAudit.toJson))
              .mapLeft(_ => errorResponse(Results.InternalServerError, detail = Some("auditServiceInternalError")))
            _ <- FutureEither(audit.recordEventStart(partnerAudit.eventId, partnerAudit.targetTID, partnerAudit.updatedByTID, partnerAudit.toJson))
              .mapLeft(_ => errorResponse(Results.InternalServerError, detail = Some("auditServiceInternalError")))
            target <- serial.map(getDeviceTargetBySerial(_, partnerUuid)).getOrElse(FutureEither.successful(Right(Target.Model(model))))
            adminRequest = PushAdminCmdToConnectedDevicesRequest(partnerId, target, command, Some(idempotencyKey), Some(expirationTime))
            response <- FutureEither(sherlock.pushAdminCmdToConnectedDevices(adminRequest, requestInfo))
          } yield response

          resp.fold(err => {
            logAuditEventFailure(auditEvents, request, userTid, "response" -> err, "correlationId" -> correlationId)
            err
          }, response => {
            if (response.code != Transmitted) {
              logAuditEventFailure(auditEvents, request, userTid, "response" -> response, "correlationId" -> correlationId)
              Results.InternalServerError(Json.obj("serviceCode" -> response.code.toString, "idempotencyKey" -> idempotencyKey))
            } else {
              logAuditEventSuccess(auditEvents, request, response, userTid)
              Ok(Json.obj("serviceCode" -> response.code.toString, "idempotencyKey" -> idempotencyKey))
            }
          })
        }
        result.fold(l => Future.successful(l), identity)
      } else {
        Future.successful(errorResponse(Results.Forbidden))
      }
    }
  }

  def dockBasedSelfRegister(): Action[String] = Action(parse.text).async { implicit req =>
    val func = "dockBasedSelfRegister"
    statsd.incrementCounter("dock_based_self_register")
    SelfService.withJwt(arkham, req, DockBasedRegisterJWTParser()) { jwt =>
      val response: FutureEither[Result, Result] = for {
        partnerId <- komrade.getPartnerIdFromDomain(jwt.domain).map(p => Convert.fromGuidToTidFormat(p.value))
        _ <- FutureEither(DeviceConfigPermissions(arsenal,
          DeviceConstants.DeviceConfig.DockBasedRegistrationConfig).ensurePartnerHasConfigEnabled(partnerId, jwt.model.value))
        //check to see if device is already registered to this (or any) agency
        //only register if non-existent or owned by the root agency
        deviceCheck <- getDeviceIfRegisteredToPartner(jwt, partnerId)
        device <- deviceCheck match {
          case Some(d) => FutureEither(Future.successful(Right(d)))
          case _ => processDockBasedRegistration(jwt, partnerId)
        }
      } yield {
        statsd.incrementCounter("dock_based_self_register_success")
        logger.info("Dock based registration successful")(
          DeviceConstants.FuncString -> func,
          DeviceConstants.AgencyIdString -> partnerId,
          DeviceConstants.DeviceIdString -> device.id)
        Created(Json.obj(dataField -> deviceInfoToJson(device)))
      }

      response.fold(l => l, r => r)
    }
  }

  def selfRegister(): Action[String] = Action(parse.text).async { implicit req =>
    val func = "selfRegister"
    statsd.incrementCounter("self_register")
    SelfService.withJwt(arkham, req) { jwt =>

      val response: FutureEither[Result, Result] = for {
        partnerId <- komrade.getPartnerIdFromDomain(jwt.domain).map(p => Convert.fromGuidToTidFormat(p.value))
        //check to see if device is already registered to this (or any) agency
        //only register if non-existent or owned by the root agency
        result <- getDeviceIfRegisteredToPartner(jwt, partnerId)
        device <- {
          val metadata: Future[Either[Result, DeviceMetaData]] = result match {
            case Some(d) => Future.successful(Right(d))
            case None =>
              val request = RegisterDeviceRequest(partnerId
                , jwt.serialNumber.value
                , jwt.model.value
                , partnerEntityDescriptor(rootPartnerIdTidFormat)
                , forceMoveDeviceFromAnotherAgency = Some(false))
              registerDevice(request)
          }

          FutureEither(metadata)
        }
      } yield {
        statsd.incrementCounter("self_register_success")
        logger.info("Registration successful")(
          DeviceConstants.FuncString -> func,
          DeviceConstants.AgencyIdString -> partnerId,
          DeviceConstants.DeviceIdString -> device.id)
        Created(Json.obj(dataField -> deviceInfoToJson(device)))
      }

      response.fold(l => l, r => r)
    }
  }

  def selfToken(agencyId: String): Action[String] = {
    val func = "selfToken"
    statsd.incrementCounter("self_token")
    Action(parse.text).async { implicit req =>
      SelfService.withJwt(arkham, req)({ jwt =>

        val tokenResponse: FutureEither[Result, CreateSessionResponse] = for {
          partnerId <- {
            val pid: Either[Result, PartnerID] = Convert.toUuid(agencyId).map(u => Right(PartnerID(u))).getOrElse(Left(Results.BadRequest))
            FutureEither.successful(pid)
          }
          deviceMetadata <- {
            val request = GetDeviceBySerialQuery(serial = jwt.serialNumber.value, partnerId = agencyId)

            val device: Future[Either[Result, (String, String)]] = arsenal.getDeviceBySerial(request).map {
              case Right(deviceResponse) => (for {
                d <- deviceResponse.device
                id <- d.id
                model <- d.model
              } yield Right((id, model))).getOrElse(Left(errorResponse(Results.NotFound)))
              case Left(code) => Left(arsenalErrorToResponse(code))
            }
            FutureEither(device)
          }
          tokenRequest <- {
            val (deviceId, deviceModel) = deviceMetadata
            val sessionData = Map(SessionDataKeys.deviceModel -> deviceModel)
            val result = DeviceObjects.Auth.createDeviceSessionRequest(deviceModel, deviceId, partnerId, Some(sessionData)).left.map { unsupported =>
              logger.warn("unsupportedModel")(
                DeviceConstants.MessageString -> "Self Token requested for unsupported model.",
                "partnerId" -> partnerId.value,
                DeviceConstants.ModelString -> unsupported,
                DeviceConstants.DeviceIdString -> deviceId)
              errorResponse(Results.MethodNotAllowed)
            }
            FutureEither.successful(result)
          }
          sessionResponse <- {
            val response: Future[Either[Result, CreateSessionResponse]] = sessions.createDeviceSession(tokenRequest).map {
              case Right(r@CreateSessionResponse(_, _, Some(_))) => Right(r)
              case Right(_) => Left(errorResponse(Results.Unauthorized))
              case Left(l) => l match {
                case SessionsServiceErrorCode.AuthenticationFailed => Left(errorResponse(Results.Unauthorized))
                case _ => Left(errorResponse(Results.InternalServerError))
              }
            }
            FutureEither(response)
          }
        } yield {
          statsd.incrementCounter("self_token_success")
          val (deviceId, _) = deviceMetadata
          logger.info("Authentication successful")(
            DeviceConstants.FuncString -> func,
            DeviceConstants.AgencyIdString -> partnerId,
            DeviceConstants.DeviceIdString -> deviceId)
          sessionResponse
        }
        tokenResponse.fold(l => l, response => {
          buildTokenResponse(response)
        })
      })
    }
  }



  def update(maybeAgencyId: String, id: String): Action[JsValue] = {
    val func = "update"
    authenticateViaToken.async(parse.tolerantJson) { authenticatedRequest =>
      Convert.tryToUuid(maybeAgencyId).map { agencyId =>
        logger.debug(func)()
        if (agencyMatchesAuthenticatedUserAgency(agencyId.toString, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]])) {
          val updatedBy = updatedByFromJwt(authenticatedRequest.jwt)
          val result = for {
            deviceResponse <- FutureEither(getDeviceByIdWrapper(agencyId.toString, id))
            _ <- FutureEither(filterModelsFromResultFutureEither(deviceResponse.device.flatMap(_.model), DeviceConstants.SnfFilterSet) {
              Future.successful(Right(()))
            })
            request <- FutureEither.successful(validateUpdateRequest(parseDeviceUpdateRequest(authenticatedRequest.request.body, Some(id))))
            responses <- updateDeviceOwnerAndName(agencyId, deviceResponse, request, authenticatedRequest, id, updatedBy)
            (assignDeviceResponse, setDeviceNameResponse) = responses
            setDevicePrefsResponse <- updateDevicePreferences(agencyId, deviceResponse, request, authenticatedRequest, id, updatedBy)
            device <- FutureEither(updateResponsesToDevice(assignDeviceResponse, setDevicePrefsResponse, setDeviceNameResponse, agencyId, id))
          } yield {
            val model = deviceResponse.device.flatMap(_.model).getOrElse("")
            if (DeviceConstants.isConnectedModel(model) && (assignDeviceResponse.device.isDefined || setDevicePrefsResponse.device.isDefined)) {
              pushFullConfigToDevice(agencyId.toString, id)
            }

            if (DeviceConstants.ModelsWithDeviceStatesInElasticsearch.contains(model) && assignDeviceResponse.device.isDefined) {
              val reindexRequest = ReindexDeviceOwnerRequest(agencyId.toString, id)
              panoptes.reindexDeviceOwner(reindexRequest)
            }

            Ok(Json.obj(dataField -> deviceInfoToJson(device)))
          }
          result.fold(l => l, r => r)
        } else {
          Future.successful(errorResponse(Results.Forbidden))
        }
      }.getOrElse(Future.successful(errorResponse(Results.BadRequest)))
    }
  }

  def updateDeviceNames(maybeAgencyId: String, newName: Option[String]): Action[JsValue] = {
    val func = "updateDeviceNames"
    authenticateViaToken.async(parse.tolerantJson) { authenticatedRequest =>
      executionTime(func) {
        val finalResult: FutureEither[Result, Result] =
          for {
            agencyId <- FutureEither.successful(Convert.tryToUuid(maybeAgencyId).toRight(errorResponse(Results.BadRequest)))

            // validate the agency domain
            _ <- FutureEither.successful {
              if (agencyMatchesAuthenticatedUserAgency(agencyId.toString, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]])) {
                Right(())
              } else {
                Left(errorResponse(Results.Forbidden))
              }
            }

            newFriendlyName <- FutureEither.successful(validateFriendlyName(newName))
            updatedBy <- FutureEither.successful(Right(updatedByFromJwt(authenticatedRequest.jwt)))
            deviceUUIDs <- FutureEither.successful(validateDeviceIdsFromRequestPayload(authenticatedRequest.request.body))
            deviceIds <- FutureEither.successful(Right(deviceUUIDs.map(_.toString)))

            _ <- FutureEither.successful {
              logger.info(func)(
                DeviceConstants.FuncString -> func,
                DeviceConstants.AgencyIdString -> agencyId,
                DeviceConstants.CountString -> deviceIds.size
              )
              if (deviceIds.length > DeviceConstants.BulkUpdateLimit)
                Left(bulkUpdateLimitExceededError)
              else
                Right(())
            }

            devicesWithHttpStatusMetadata <- {
              def permissionValidation(deviceMetaData: Option[DeviceMetaData]): Either[Result, Unit] =
                validatePermissionToModifyDeviceAgency(agencyId, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]], deviceMetaData)

              FutureEither(validatePermissionToModifyDeviceMetadata(agencyId, authenticatedRequest, deviceIds, permissionValidation))
            }

            prohibitedDevices <- FutureEither.successful(Right(devicesWithHttpStatusMetadata.filter(_.httpStatusMetadata.resultStatus != Results.Ok)))

            devicesToUpdate <- FutureEither.successful(Right(devicesWithHttpStatusMetadata.filter(_.httpStatusMetadata.resultStatus == Results.Ok)))

            devices <- processBulkRename(agencyId, updatedBy, newFriendlyName, devicesToUpdate)
          } yield {
            Ok(deviceWithErrorListJsonResponse(devices, prohibitedDevices))
          }

        finalResult.fold(l => l, r => r)
      }
    }
  }

  private def processBulkRename(agencyId: UUID, updatedBy: EntityDescriptor, newFriendlyName: String,
                 devicesToUpdate: Seq[DeviceWithHttpStatusMetadata]): FutureEither[Result, Seq[DeviceWithErrorCode]] = {
    val func = "processBulkRename"
    val devicesWithErrorCodeFutures = {
      devicesToUpdate.map(deviceToUpdate => {
        val thriftRequest = SetDeviceNameRequest(agencyId.toString, deviceToUpdate.id, updatedBy, newFriendlyName)
        arsenal.setDeviceName(thriftRequest).map{
          case Left(error) => DeviceWithErrorCode(deviceToUpdate.id, error, deviceToUpdate.device)
          case Right(resp) => DeviceWithErrorCode(deviceToUpdate.id, ServiceErrorCode.Ok, resp.device)
        }.recover {
          case ex => {
            logger.error(ex, func) (
              DeviceConstants.AgencyIdString -> agencyId,
              DeviceConstants.DeviceIdString -> deviceToUpdate.id
            )
            DeviceWithErrorCode(deviceToUpdate.id, ServiceErrorCode.Exception, deviceToUpdate.device)
          }
        }
      })
    }

    FutureEither(Future.sequence(devicesWithErrorCodeFutures).map {
      case Nil => Left(errorResponse(Results.InternalServerError))
      case devices => Right(devices)
    })
  }

  private def updateFirmwareImpl(agencyId: UUID, deviceId: UUID, newVersion: String): Future[Either[Result, Unit]] = {
    val setDeviceFirmwareVersionRequest = SetDeviceFirmwareVersionRequest(agencyId.toString, deviceId.toString, newVersion)
    val promise = arsenal.setDeviceFirmwareVersion(setDeviceFirmwareVersionRequest)
    for (resp <- promise) yield {
      resp match {
        case Left(error) => Left(arsenalErrorToResponse(error))
        case Right(_) => Right(())
      }
    }
  }

  private def getDeviceIdBySerial(agencyUuid: UUID, serial: String): FutureEither[Result, String] = {
    for {
      deviceMetaData <- FutureEither(
        getDeviceInternal(agencyUuid, GetDeviceBySerial(serial), None)
      )
    } yield {
      deviceMetaData.id.getOrElse(DeviceConstants.EmptyString)
    }
  }

  def updateFirmware(maybeAgencyId: String, id: String, newVersion: String, bySerial: Option[Boolean]): Action[AnyContent] = {
    authenticateViaToken.async { authenticatedRequest =>
      val result =
        for {
          agencyUuid <- FutureEither.successful(validateAgencyId(maybeAgencyId))
          maybeDeviceId <- if (bySerial.getOrElse(false)) getDeviceIdBySerial(agencyUuid, id) else FutureEither.successful(Right(id))
          deviceUuid <- FutureEither.successful(validateDeviceId(maybeDeviceId))
          _ <- FutureEither.successful(validateFirmwareVersion(newVersion))
          _ <- {
            if (hasFleetDevicePermission(agencyUuid, authenticatedRequest)) {
              FutureEither.successful(Right(()))
            } else {
              FutureEither.successful(Left(Results.Forbidden))
            }
          }
          _ <- FutureEither(updateFirmwareImpl(agencyId = agencyUuid, deviceId = deviceUuid, newVersion = newVersion))
        } yield {
          Results.Ok
        }

      result.fold(l => l, r => r)
    }
  }



  /**
    * This method does the following,
    * 1] Parse and validate the request (The request contains a list of device ID's in the payload)
    * 2] Fetch devices from arsenal (This is a single thrift call)
    * 3] Check device modify permissions
    * 4] Create 2 list's,
    *   - first list for devices for which permission check failed (These devices will have an error code of "Operation Not Permitted" in the final response)
    *   - second list for devices that need to be updated
    * 5] Call arsenal to update device status for all devices that can be updated
    * 6] Arsenal returns a list of devices along with a service error code that indicates whether the operation was successful or not for the corresponding device
    * 7] Combine the response for devices with permission errors and devices returned from arsenal
    *
    * @param maybeAgencyId The agencyUuid for the devices that need to be updated
    * @param newStatus     The target device status for the specified list of devices
    * @return
    */
  def updateStatus(maybeAgencyId: String, newStatus: Option[String]): Action[JsValue] = {
    authenticateViaToken.async(parse.tolerantJson) { authenticatedRequest =>
      val finalResult: FutureEither[Result, Result] =
        for {
          agencyId <- FutureEither.successful(Convert.tryToUuid(maybeAgencyId).toRight(errorResponse(Results.BadRequest)))

          // validate the agency domain
          _ <- FutureEither.successful {
            if (agencyMatchesAuthenticatedUserAgency(agencyId.toString, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]])) {
              Right(())
            } else {
              Left(errorResponse(Results.Forbidden))
            }
          }

          updatedBy <- FutureEither.successful(Right(updatedByFromJwt(authenticatedRequest.jwt)))

          request <- FutureEither.successful(validateDeviceStatusUpdateRequest(authenticatedRequest.request.body, newStatus))

          devicesWithHttpStatusMetadata <- {
            def permissionValidation(deviceMetaData: Option[DeviceMetaData]): Either[Result, Unit] =
              validatePermissionToModifyDeviceAgency(agencyId, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]], deviceMetaData)
            FutureEither(validatePermissionToModifyDeviceMetadata(agencyId, authenticatedRequest, request.deviceIds, permissionValidation))
          }

          prohibitedDevices <- FutureEither.successful(Right(devicesWithHttpStatusMetadata.filter(_.httpStatusMetadata.resultStatus != Results.Ok)))

          devicesToUpdate <- FutureEither.successful(Right(devicesWithHttpStatusMetadata.filter(_.httpStatusMetadata.resultStatus == Results.Ok)))

          devicesWithErrorFromResponse <- {
            val rv: FutureEither[Result, Seq[DeviceWithErrorCode]] =
              if (devicesToUpdate.nonEmpty) {
                val devicesWithStatus: Seq[ThriftDeviceIdWithStatus] =
                  devicesToUpdate.map { device =>
                    ThriftDeviceIdWithStatus(deviceId = device.id, status = request.newStatus)
                  }
                val thriftRequest = SetDeviceStatusRequestV2(partnerId = agencyId.toString, devicesWithStatus = devicesWithStatus, updatedBy = updatedBy)

                val result: Future[Either[Result, Seq[DeviceWithErrorCode]]] = arsenal.setDeviceStatusV2(thriftRequest).map {
                  case Left(error) if (error == ServiceErrorCode.LimitExceeded) => Left(bulkUpdateLimitExceededError)
                  case Left(error) => Left(arsenalErrorToResponse(error))
                  case Right(response) => {
                    response.devicesWithErrorCode.toRight(errorResponse(Results.InternalServerError))
                  }
                }

                FutureEither(result)
              } else {
                FutureEither.successful(Right(Nil))
              }

            rv
          }
        } yield {
          Ok(deviceWithErrorListJsonResponse(devicesWithErrorFromResponse, prohibitedDevices))
        }

      finalResult.fold(l => l, r => r)
    }
  }

  /**
    * Update the device state information for the given serial and model. Currently only supports models:
    * taser_etm_flex_2
    * @param model device model
    * @param serial serial number of device
    * @return
    */
  def updateState(model: String, serial: String): Action[AnyContent] = {

    def validateSerial: FutureEither[Result, Unit] = {
      FutureEither.successful { // validate the serial number
        if (StringUtils.isNullOrWhiteSpace(serial)) {
          Left(errorResponse(Results.BadRequest, detail = Some("Serial not provided")))
        } else {
          Right(())
        }
      }
    }

    def extractStateData(requestBody: AnyContent): FutureEither[Result, String] = {
      FutureEither.successful {
        model match {
          case AxonDockModel =>
            requestBody.asFormUrlEncoded.flatMap { form =>
              form.get(DeviceConstants.DockStateBase64EncKey).map { data =>
                if(data.isEmpty){
                  Left(errorResponse(resultStatus = Results.BadRequest, detail = Some("no data provided")))
                } else if (data.head.length > DeviceConstants.MaxEtmFlex2StateLengthAllowed) {
                  Left(errorResponse(resultStatus = Results.BadRequest, detail = Some("Data too large")))
                } else {
                  Right(data.head)
                }
              }
            }.getOrElse(Left(errorResponse(resultStatus = Results.BadRequest, detail = Some("State data not provided"))))

          case _ => Left(errorResponse(Results.BadRequest, detail = Some("Model not supported")))
        }
      }
    }

    def sendToKafkaProducer(key: String, data: String): FutureEither[Result, RecordMetadata] = {
      model match {
        case AxonDockModel => FutureEither {
          kafka.sendToEtmFlex2StateProducer(key, data)
            .map(Right(_))
            .recover {
              case NonFatal(_) => Left(errorResponse(Results.InternalServerError, detail = Some("unable to store message")))
            }
        }

        case _ => FutureEither.successful(Left(errorResponse(Results.BadRequest, detail = Some("Model not supported"))))
      }
    }

    authenticateViaToken.async{ req =>
      val result = for {
        _ <- validateSerial
        data <- extractStateData(req.body)
        partner <- FutureEither(domainNameResolver.getPartnerFromDomainName(req.host))
        _ <- sendToKafkaProducer( Etm2StateKey(partner.id, serial).toJsonString, data)
      } yield {
        getEmptyResponse(DeviceConstants.UpdateStateTypeString)
      }
      result.fold(l => l, r => r)
    }
  }

  def getRegistrationStatus(serial: String): Action[String] = Action(parse.text).async { implicit req =>

    val ab4DockMask = Model.AxonBody4Dock1Bay.masks ++ Model.AxonBody4Dock8Bay.masks

    def getIsFirstRegister(partnerId: String, entityId: String): Future[Either[Result, Option[Boolean]]] = {
      val request = GetEntityBasedRegisterTimeRequest(partnerId, entityId)
      val farthestValidRegistrationTime = System.currentTimeMillis() - DeviceConstants.ValidDockBasedRegistrationTimeWindow
      arsenal.getEntityBasedRegisterTime(request).map {
        case Right(r) => Right(Some(r.timestamp.getOrElse(0.toLong) < farthestValidRegistrationTime))
        case Left(l) => l match {
          case ServiceErrorCode.NotFound => Right(Some(true)) // no record means first time register
          case _ => Left(arsenalErrorToResponse(l))
        }
      }
    }

    def registrationtatusToJson(metadata: DeviceMetaData, agencyDomain: String, isFirstRegister: Option[Boolean]): JsObject = {
      val baseJs = Json.obj(
        DeviceConstants.RegistrationStatusRespPayloadKeys.Serial -> metadata.serial,
        DeviceConstants.RegistrationStatusRespPayloadKeys.AgencyDomain -> agencyDomain,
        DeviceConstants.RegistrationStatusRespPayloadKeys.AgencyUuid -> metadata.partnerId,
        // As the device exists in our DB, hard-coded as REGISTERED until DEREGISTERED supported
        DeviceConstants.RegistrationStatusRespPayloadKeys.Status -> DeviceConstants.RegistrationStatus.Registered)
      val finalJs = isFirstRegister match {
        case Some(value) => baseJs ++ Json.obj(
          DeviceConstants.RegistrationStatusRespPayloadKeys.IsFirstRegister -> value)
        case None => baseJs
      }
      finalJs
    }

    val func = "registrationStatus"
    statsd.incrementCounter("registration_status")
    SelfService.withJwt(arkham, req) { jwt =>
      val response: FutureEither[Result, Result] = for {
        deviceData <- FutureEither(getDeviceIfRegistered(serial))
        _ <- FutureEither(Future.successful(Either.cond(allowedDockBasedRegisterStatus.contains(deviceData.status2.get), Unit, Results.Forbidden)))
        partnerId = deviceData.partnerId.get
        _ <- FutureEither(DeviceConfigPermissions(arsenal,
          DeviceConstants.DeviceConfig.DockBasedRegistrationConfig).ensurePartnerHasConfigEnabled(partnerId, jwt.model.value))
        partner <- FutureEither(komrade.getPartnerFromId(partnerId))
        isFirstRegister <- FutureEither(ab4DockMask.exists(serial.startsWith) match {
          case true => getIsFirstRegister(partnerId, deviceData.id.get)
          case false => Future.successful(Right(None))
        })
      } yield {
        statsd.incrementCounter("registration_status_success")
        logger.info("Get registration status successful")(
          DeviceConstants.FuncString -> func,
          DeviceConstants.SerialString -> serial)
        Ok(Json.obj(dataField -> registrationtatusToJson(deviceData, partner.domain.get, isFirstRegister)))
      }
      response.fold(l => l, r => r)
    }
  }

  def getAssociatedDevicesByParentSerial(maybeAgencyId: String, parentSerial: String): Action[AnyContent] = {
    security.authorize(getSecAuthorizeDeviceReadPermission(maybeAgencyId)).async { authenticatedRequest =>
      logger.debug("getAssociatedDevicesByParentSerial")("agencyId" -> maybeAgencyId, "parentSerial" -> parentSerial)

      Convert.tryToUuid(maybeAgencyId).map { agencyId =>
        val getRequest = GetAssociatedDevicesByParentSerialQuery(agencyId.toString, parentSerial)
        val promise = arsenal.getAssociatedDevicesByParentSerial(getRequest)

        for { resp <- promise } yield {
          resp match {
            case Left(error) => arsenalErrorToResponse(error)
            case Right(getResponse) => getResponse.devices.map(devices => Ok(deviceListJsonResponse(devices))).getOrElse(errorResponse(Results.InternalServerError))
          }
        }
      }.getOrElse(Future.successful(BadRequest))
    }
  }

  def disassociateDeviceFromParent(maybeAgencyId: String, childDeviceSerial: String): Action[AnyContent] = {
    security.authorize(getSecAuthorizeDeviceWritePermission(maybeAgencyId)).async { authenticatedRequest =>
      logger.debug("disassociateDeviceFromParent")("agencyId" -> maybeAgencyId, "childDeviceSerial" -> childDeviceSerial)

      Convert.tryToUuid(maybeAgencyId).map { agencyId =>
        val updatedBy = updatedByFromJwt(authenticatedRequest.jwt)
        val disassociateRequest = DisassociateDeviceFromParentRequest(agencyId.toString, childDeviceSerial, Some(updatedBy))
        val promise = arsenal.disassociateDeviceFromParent(disassociateRequest)

        for { resp <- promise } yield {
          resp match {
            case Left(error) => arsenalErrorToResponse(error)
            case Right(getResponse) => Ok
          }
        }
      }.getOrElse(Future.successful(BadRequest))
    }
  }

  def getParentDeviceByAssociatedSerial(maybeAgencyId: String, childDeviceSerial: String): Action[AnyContent] = {
    security.authorize(getSecAuthorizeDeviceReadPermission(maybeAgencyId)).async { authenticatedRequest =>
      logger.debug("getParentDeviceByAssociatedSerial")("agencyId" -> maybeAgencyId, "childDeviceSerial" -> childDeviceSerial)

      Convert.tryToUuid(maybeAgencyId).map { agencyId =>
        val getRequest = GetParentDeviceByAssociatedSerialQuery(agencyId.toString, childDeviceSerial)
        val promise = arsenal.getParentDeviceByAssociatedSerial(getRequest)

        for { resp <- promise } yield {
          resp match {
            case Left(error) => arsenalErrorToResponse(error)
            case Right(getResponse) => getResponse.device.map(parent => Ok(deviceInfoToJson(parent))).getOrElse(errorResponse(Results.InternalServerError))
          }
        }
      }.getOrElse(Future.successful(BadRequest))
    }
  }

  /*
   * End of device association endpoints.
   */


  private def assignDevice(authenticatedRequest: JWTAuthenticatedRequest[JsValue], agencyId: String, id: String, request: DeviceUpdateRequest, updatedBy: EntityDescriptor): Future[Either[Result, AssignDeviceResponse]] = {
    val func = "assignDevice"
    logger.debug(func)()

    request.ownerId.map { _ =>
      val owner = getOwnerFromRequest(authenticatedRequest.jwt.audienceId, request)
      val assignRequest = AssignDeviceRequest(agencyId, id, owner.get, updatedBy)

      arsenal.assignDevice(assignRequest).map {
        case Left(err) => Left(arsenalErrorToResponse(err))
        case Right(resp) => Right(resp)
      }
    }.getOrElse(Future.successful(Right(AssignDeviceResponse(None))))
  }

  private def assignDeviceHomeImpl(agencyId: UUID, homeId: UUID, devices: Seq[DeviceWithHttpStatusMetadata], updatedBy: EntityDescriptor): Future[Either[Result, Seq[DeviceWithErrorCode]]] = {
    if (devices.nonEmpty) {

      def arsenalResponseForAssigningDeviceHome(): Future[Either[Result, AssignDeviceHomeResponse]] = {
        val request = AssignDeviceHomeRequest(partnerId = agencyId.toString, deviceHomeId = homeId.toString, deviceIds = devices.map(_.id), updatedBy = updatedBy)
        arsenal.assignDeviceHome(request).map {
          case Left(error) => Left(arsenalErrorToResponse(error))
          case Right(resp) => Right(resp)
        }
      }

      def getDeviceMetadataListFromArsenalResponse(arsenalResponse: AssignDeviceHomeResponse): Either[Result, Seq[DeviceMetaData]] = {
        arsenalResponse.devices
          .filter(_.nonEmpty)
          .toRight(
            errorResponse(resultStatus = Results.InternalServerError, detail = Some("Device metadata missing in assign device home response"))
          )
      }

      val result: FutureEither[Result, Seq[DeviceWithErrorCode]] =
        for {
          arsenalResponse <- FutureEither(arsenalResponseForAssigningDeviceHome())
          deviceMetadataList <- FutureEither.successful(getDeviceMetadataListFromArsenalResponse(arsenalResponse))
        } yield {
          getDevicesWithErrorCode(deviceMetadataList)
        }

      result.fold(l => Left(l), r => Right(r))
    } else {
      Future.successful(Right(Nil))
    }
  }

  private def buildTokenResponse(response: CreateSessionResponse): Result = {
    val tryParse = for {
      dateExpires <- Try {
        ZonedDateTime.parse(response.authorization.dateExpires)
      }
      dateNotBefore <- Try {
        ZonedDateTime.parse(response.authorization.dateNotBefore)
      }
    } yield (dateExpires, dateNotBefore)

    tryParse.map(parsed => {
      val (dateExpires, dateNotBefore) = parsed
      val entityMetadata = EntityMetadata(
        response.authorization.entity.entityType.name,
        response.authorization.entity.id.toString,
        response.authorization.entity.domain.map(_.toString).getOrElse(""))
      val tokenMetadata = TokenMetadata(
        response.token.map(_.toString).getOrElse(""),
        response.tokenType.toString,
        dateExpires.toInstant.toEpochMilli.toString,
        dateNotBefore.toInstant.toEpochMilli.toString,
        entityMetadata)
      Ok(Json.toJsObject(tokenMetadata))
    })
    .getOrElse {
      logger.error("Failed to parse expiration date/not before date.")(
        "expiration date" -> response.authorization.dateExpires, "not before date" -> response.authorization.dateExpires)
      errorResponse(Results.InternalServerError)
    }
  }

  private def constructRegisterRequest(agencyId: String, json: JsValue, authenticatedRequest: JWTAuthenticatedRequest[JsValue]): Option[RegisterDeviceRequest] = {
    val createRequest = validateCreateRequest(parseDeviceUpdateRequest(json))
    if (createRequest.isDefined) {
      val owner = getOwnerFromRequest(authenticatedRequest.jwt.audienceId, createRequest.get)
      val updatedBy = updatedByFromJwt(authenticatedRequest.jwt)
      val forceMove = authenticatedRequest.getQueryString("force_move").map(_.toBoolean)

      Option(RegisterDeviceRequest(
        partnerId = agencyId,
        serial = createRequest.get.serial.get,
        model = createRequest.get.model.get,
        updatedBy = updatedBy,
        make = createRequest.get.make,
        friendlyName = createRequest.get.friendlyName,
        owner = owner,
        forceMoveDeviceFromAnotherAgency = forceMove,
        dateLastUpload = createRequest.get.dateLastUpload.map(ZonedDateTime.parse(_).toInstant.toEpochMilli)))
    } else {
      None
    }
  }

  private def deviceNotFoundJsonResponse(): Result = {
    NotFound(Json.obj(errorsField -> DeviceConstants.DeviceNotFoundErrorMessage))
  }

  private def filterDeviceModelsAndGetDeviceMetadata(deviceResp: Either[ServiceErrorCode, GetDeviceResponse]): Future[Either[Result, DeviceMetaData]] = {
    filterDeviceModels(deviceResp).map {
      case Left(error) => Left(error)
      case Right(resp) => resp.device.toRight(deviceNotFoundJsonResponse())
    }
  }

  private def filterDeviceModels(deviceResp: Either[ServiceErrorCode, GetDeviceResponse]): Future[Either[Result, GetDeviceResponse]] = {
    deviceResp match {
      case Left(error) => Future.successful(Left(arsenalErrorToResponse(error)))
      case Right(response) =>
        filterModelsFromResultFutureEither(response.device.flatMap(_.model), DeviceConstants.SnfFilterSet) {
          response.device
          .map(_ => Future.successful(Right(response)))
          .getOrElse(Future.successful(Left(deviceNotFoundJsonResponse())))
        }
    }
  }

  private def filterModelsFromResultFutureEither[A](model: Option[String], blacklist: Set[String])(body: => Future[Either[Result, A]]): Future[Either[Result, A]] =
    filterModels(model, blacklist)(body)(result => Future(Left(result)))

  private def getAgencyFirmwareVersions(
    agencyId: UUID,
    model: String,
    status2: Option[String]
  ): Future[Either[Result, GetAgencyFirmwareVersionsResponse]] = {
    val deviceQueryParams =
      DeviceQueryParams(
        partnerId = agencyId,
        model = Some(model),
        limit = Some(1),
        facetPivot = Some(Seq(DeviceConstants.DeviceFirmwareString)),
        status2 = status2
      )

    val deviceSearchClient = DeviceSearchClient(
      agencyId = agencyId,
      solrClient = solrClient
    )

    val futureErrorOrResponse =
      getSolrResponse(deviceQueryParams, deviceSearchClient).map {
        case Right(solrResponse) => processSolrResponseFirmware(solrResponse)
        case Left(searchError) => {
          val errResponse =
            errorResponse(
              resultStatus = Results.InternalServerError,
              detail = Some(s"Problem with '${FacetPivotResponseString}' processing.  (SearchError = ${searchError})")
            )
          Left(errResponse)
        }
      }

    futureErrorOrResponse
  }

  private def getAuditDates(startDate: Option[String], endDate: Option[String]): Either[Result, Map[String, Seq[String]]] = {
    val func = "getAuditDates"
    val ecomsaasFormat = DateTimeFormat.forPattern("dd MMM yyyy HH:mm:ss")
    val reporterFormat = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss a")
    try {
      val formattedStartDate = startDate match {
        case None => new DateTime(0).toString(reporterFormat)
        case Some(d) => ecomsaasFormat.parseDateTime(d).toString(reporterFormat)
      }
      val formattedEndDate = endDate match {
        case None => new DateTime(DateTimeZone.UTC).plusDays(1).toString(reporterFormat)
        case Some(d) => ecomsaasFormat.parseDateTime(d).toString(reporterFormat)
      }

      Right(Map(DeviceConstants.DateTimeStartString -> Seq(formattedStartDate), DeviceConstants.DateTimeEndString -> Seq(formattedEndDate)))
    } catch {
      case _: IllegalArgumentException | _: UnsupportedOperationException =>
        logger.info(func)(
          DeviceConstants.MessageString -> "invalid start or end date", DeviceConstants.StartDateString -> startDate, DeviceConstants.EndDateString -> endDate)
        Left(BadRequest)
    }
  }

  private def getCewModifyPermission(agencyId: UUID, device: DeviceMetaData): (Permissions, Map[String, Object]) = {
    val params = Map(Permissions.partnerAccess -> agencyId.toString)
    val ownerId = device.owner.map(_.id).getOrElse("")
    val permission = Permissions(
      Or(
        And(IsActor(ownerId), Scope(ECOMScopes.CEW_SELF_MODIFY)),
        Scope(ECOMScopes.CEW_ANY_MODIFY)
      )
    )
    (permission, params)
  }

  private def getCurrentFirmwareVersion(
    agencyId: UUID,
    model: String
  ): Future[Either[Result, GetLatestFirmwareVersionResponse]] = {
    val request = GetPartnerLatestFirmwareVersionRequest(
      partnerId = agencyId.toString,
      deviceModel = model
    )

    for {
      response <- arsenal.getPartnerLatestFirmwareVersion(request)
    } yield {
      response match {
        case Right(getLatestFirmwareVersionResponse) => Right(getLatestFirmwareVersionResponse)
        case Left(serviceErrorCode) => Left(arsenalErrorToResponse(serviceErrorCode))
      }
    }
  }

  /**
   * Get functional test stats and return a JSON object of the stats.
   * @param partnerId
   * @param models
   * @param numberOfDays
   * @return JSON response
   */
  private def getFunctionalTestStats(
    partnerId: UUID,
    models: Set[String],
    baseModel: String,
    numberOfDays: Int,
    status2: Option[String]
  ): Future[Either[Result, JsObject]] = {
    val func = "getFunctionalTestStats"

    val functionalTestStatsQueries = DeviceSearchClient.getFunctionalTestStatsQueries(numberOfDays.toString)

    val commaSeparatedModels = models.mkString(DeviceConstants.CommaChar.toString)
    val deviceQueryParams =
      DeviceQueryParams(
        partnerId = partnerId,
        model = Some(commaSeparatedModels),
        limit = Some(0), // don't need any rows of data. just the facet response is important
        facetQueries = Some(functionalTestStatsQueries),
        status2 = status2
      )

    val futureErrorOrSolrResponse = getSolrResponse(deviceQueryParams, DeviceSearchClient(partnerId, solrClient))

    val futureErrorOrResponse: Future[Either[Result, JsObject]] =
      futureErrorOrSolrResponse.map {
        case Right(solrResponse) =>
          Try { // Try block to protect against Int conversion
            solrResponse.facets.map { facets =>
              val queryResults = functionalTestStatsQueries.keys.map { name =>
                val count = (facets \ name \ SolrConstants.CountString).asOpt[Int]
                (name, count)
              }.toSeq

              deviceFunctionalTestStatsToJson(model = commaSeparatedModels, models = models, baseModel = baseModel, numberOfDays = numberOfDays, results = queryResults)

            }.getOrElse(throw InternalServerException("Facet response not found"))
          } match {
            case Success(response) => Right(response)
            case Failure(ex) =>
              val message = "Problem with facet result"
              logger.error(ex, func)("message" -> message)
              Left(errorResponse(Results.InternalServerError, detail=Some(message)))
          }

        case Left(_) => Left(errorResponse(Results.InternalServerError, detail=Some("Problem querying functional test stats")))
      }

    futureErrorOrResponse
  }

  private def getDeviceEDCAPermission: (Permissions, Map[String, Object]) = {
    val params = Map(Permissions.partnerAccess -> "00000000-0000-0000-0000-000000000000")
    val permission = Permissions(Scope(ECOMScopes.DEVICE_ANY_READ))
    (permission, params)
  }

  private def getDeviceIfRegistered(serial: String): Future[Either[Result, DeviceMetaData]] = {
    val request = GetDeviceBySerialOnlyQuery(serial)
    arsenal.getDeviceBySerialOnly(request).map {
      case Right(r) =>
        (for {
          device <- r.device
          ownerED <- device.owner
          ownerPartnerId <- Try(Convert.fromGuidToTidFormat(ownerED.partnerId)).toOption
          _ <- if (ownerPartnerId.equalsIgnoreCase(rootPartnerIdTidFormat)) None else Some(Unit)
        } yield {
          device
        }).toRight(errorResponse(Results.NotFound))
      case Left(l) => Left(arsenalErrorToResponse(l))
    }
  }

  private def getDeviceIfRegisteredToPartner(jwt: DeviceJWT, partnerId: String) = {
    val request = GetDeviceBySerialOnlyQuery(serial = jwt.serialNumber.value)
    val isUnRegistered = arsenal.getDeviceBySerialOnly(request).map {
      case Right(r) => {
        (for {
          device <- r.device
          ownerED <- device.owner
          ownerPartnerId <- Try(Convert.fromGuidToTidFormat(ownerED.partnerId)).toOption
        } yield {
          val canRegister: Either[Result, Option[DeviceMetaData]] = if (ownerPartnerId.equalsIgnoreCase(rootPartnerIdTidFormat)) {
            Right(None) //owned by root
          } else if (ownerPartnerId.equalsIgnoreCase(partnerId)) {
            Right(Some(device)) //owned by query agency
          } else {
            Left(errorResponse(Results.Forbidden)) //owned by someone other than root
          }

          if (!device.model.exists(model => jwt.model.value.equalsIgnoreCase(model))) {
            statsd.incrementCounter("mismatched_model_count")
            logger.warn("mismatchedModelWarning")(
              "message" -> "A device is registered as a mismatching model.",
              "serialNumber" -> request,
              "ownerPartnerId" -> ownerPartnerId,
              "currentModel" -> device.model.getOrElse("None"),
              "userRequestPartnerId" -> partnerId,
              "userRequestModel" -> jwt.model.value)
          }

          canRegister
        }).getOrElse(Right(None)) //non-existent new device
      }
      case Left(l) => l match {
        case ServiceErrorCode.NotFound => Right(None) //also non-existent
        case _ => Left(arsenalErrorToResponse(l))
      }
    }
    FutureEither(isUnRegistered)
  }

  private def getDeviceImpl(maybeAgencyId: String, getDeviceRequest: GetDeviceRequest): Action[AnyContent] = {
    def getDeviceMetadata(agencyId: UUID, fields: Option[Fields]): Future[Either[Result, DeviceMetaData]] = {
      for {
        deviceResp <- getResponseForGetDeviceRequest(agencyId, getDeviceRequest, fields)
        deviceMetadata <- filterDeviceModelsAndGetDeviceMetadata(deviceResp)
      } yield {
        deviceMetadata
      }
    }

    def validateDeviceReadPermission(agencyId: UUID, authenticatedRequest: JWTAuthenticatedRequest[AnyContent], device: DeviceMetaData): Either[Result, Unit] = {
      val (permissions, params) =
        if (hasRootAgencyAccessPermission(authenticatedRequest)) {
          getDeviceEDCAPermission
        } else {
          getDeviceReadPermission(agencyId, device)
        }

      if (permissions.validate(authenticatedRequest.jwt, params)) {
        Right((): Unit)
      } else {
        Left(errorResponse(Results.Forbidden))
      }
    }

    def checkModelAndValidateDeviceReadPermission(agencyId: UUID, authReq: JWTAuthenticatedRequest[AnyContent], device: DeviceMetaData): Either[Result, Unit] = {
      val defaultCheck = validateDeviceReadPermission(agencyId, authReq, device)

      device.model.getOrElse("") match {
        case model if DeviceConstants.AirDevicesModelPrefix.exists(model.startsWith) =>
          validateAirDeviceReadPermission(agencyId, authReq) match {
            case Left(_) => defaultCheck
            case Right(r) => Right(r)
          }
        case _ => defaultCheck
      }
    }

    authenticateViaToken.async { authenticatedRequest =>
      //  isV1 is to preserve the partner API route as changes need to be made to this function.
      //  This should not be the pattern for future major route changes.
      val isV1 = authenticatedRequest.path.contains("/api/v1/")
      val isEdcaAudience = hasRootAgencyAccessPermission(authenticatedRequest)
      val result =
        for {
          agencyUuid <- FutureEither.successful(validateAgencyId(maybeAgencyId))

          deviceMetadata <- FutureEither {
            val fields = getOptionalFields(authenticatedRequest)
            getDeviceMetadata(agencyUuid, fields)
          }

          _ <- FutureEither.successful(checkModelAndValidateDeviceReadPermission(agencyUuid, authenticatedRequest, deviceMetadata))

          users <- FutureEither(getUsersFromDeviceMetadataList(agencyUuid, Seq(deviceMetadata), isEdcaAudience))

        } yield {
          val includedJson = getIncludedJson(users, deviceMetadata.deviceHome.toSeq, isV1)
          Ok(
            deviceInfoWithIncludesJson(deviceMetadata, includedJson, isEdcaAudience)
          )
        }

      result.fold(l => l, r => r)
    }
  }


  // Check in @AxonController compliant security-authorize format. Use permissions in this style if possible.
  private def getSecAuthorizeDeviceReadPermission(agencyId: String): PermissionsFilter = {
    Permissions(WithinPartner(agencyId, Scope(ECOMScopes.DEVICE_ANY_READ))).asFilter
  }

  // Check in @AxonController compliant security-authorize format. Use permissions in this style if possible.
  private def getSecAuthorizeDeviceWritePermission(agencyId: String): PermissionsFilter = {
    Permissions(WithinPartner(agencyId, Scope(ECOMScopes.DEVICE_ANY_MODIFY))).asFilter
  }

  private def getFleetDevicePermission(agencyId: UUID): (Permissions, Map[String, Object]) = {
    val params = Map(Permissions.partnerAccess -> agencyId.toString)
    val permission = Permissions(Or(Scope(ECOMScopes.DEVICE_ANY_MODIFY),Scope(ECOMScopes.VIEW_XL)))
    (permission, params)
  }

  private def getAirDeviceReadPermission(agencyId: UUID): (Permissions, Map[String, Object]) = {
    val params = Map(Permissions.partnerAccess -> agencyId.toString)
    val permission = Permissions(Scope(ECOMScopes.AIR_DEVICE_ANY_READ))
    (permission, params)
  }

  private def getDevicesWithErrorCode(deviceMetadataList: Seq[DeviceMetaData]): Seq[DeviceWithErrorCode] = {
    deviceMetadataList.flatMap { deviceMetadata =>
      for {
        deviceId <- deviceMetadata.id
      } yield {
        DeviceWithErrorCode(deviceId = deviceId, errorCode = ServiceErrorCode.Ok, device = Some(deviceMetadata))
      }
    }
  }

  private def getDeviceSearchPermission(agencyId: UUID, authenticatedRequest: JWTAuthenticatedRequest[AnyContent]): (Permissions, Map[String, Object]) = {
    // For backward-compatibility with existing API clients, allow searching if they have
    // DEVICE_ANY_READ permissions even if they don't have DEVICE_SELF_LIST permission
    val basePerm =
      if (authenticatedRequest.jwt.isSubjectAuthClient()) {
        Scope(ECOMScopes.DEVICE_ANY_READ)
      } else {
        // Fleet has both DEVICE_ANY_READ and DEVICE_SNF_ANY_READ scopes but since we are restricting DEVICE_ANY_READ only to AuthClient, we need to allow
        // fleet to search devices
        Any(getFleetHeathrowTokenPermissionCheck(), Scope(ECOMScopes.DEVICE_SELF_LIST), Scope(ECOMScopes.DEVICE_ANY_LIST))
      }

    val params = if (hasRootAgencyAccessPermission(authenticatedRequest)) {
      Map.empty[String, Object]
    } else {
      Map(Permissions.partnerAccess -> agencyId.toString)
    }

    val permission = Permissions(basePerm)
    (permission, params)
  }

  private def getEvidenceOffloadPermission(agencyId: UUID): (Permissions, Map[String, Object]) = {
    val params = Map(Permissions.partnerAccess -> agencyId.toString)
    val permission = Permissions(Scope(ECOMScopes.AXON_AWARE_ANY_CRITICAL_EVIDENCE))
    (permission, params)
  }

  private def getDeviceTargetFirmwareVersion(agencyId: UUID, deviceId: UUID): Future[Either[Result, JsObject]] = {
    val request = GetDeviceLatestFirmwareVersionRequest(partnerId = agencyId.toString, deviceId = deviceId.toString)
    arsenal.getDeviceLatestFirmwareVersion(request).map {
      case Left(e) =>
        Left(arsenalErrorToResponse(e))
      case Right(GetLatestFirmwareVersionResponse(Some(firmware))) if firmware.displayVersion.isDefined =>
        Right(deviceFirmwareToJson(agencyId, deviceId, firmware))
      case _ =>
        Left(errorResponse(Results.NotFound, detail = Some("target firmware version not found")))
    }
  }

  private def getPartnerLatestFirmwareVersion(agencyId: UUID, deviceModel: String, artifactType: Option[String], subType: Option[String]): Future[Either[Result, JsObject]] = {
    val artifactTypeUsed = artifactType.getOrElse(DeviceConstants.DefaultArtifactType)
    val subTypeUsed = subType.getOrElse(DeviceConstants.DefaultSubType)

    val request = GetPartnerLatestFirmwareVersionRequest(partnerId = agencyId.toString(), deviceModel = deviceModel, `type` = Some(artifactTypeUsed), subType = Some(subTypeUsed))
    arsenal.getPartnerLatestFirmwareVersion(request).map {
      case Left(e) =>
        Left(arsenalErrorToResponse(e))
      case Right(GetLatestFirmwareVersionResponse(Some(firmware))) if firmware.displayVersion.isDefined =>
        Right(firmwareToJson(deviceModel, artifactTypeUsed, subTypeUsed, firmware))
      case _ =>
        Left(errorResponse(Results.NotFound, detail = Some("target firmware version not found")))
    }
  }

  private def getPartnerLatestFirmware(agencyId: UUID, deviceModel: String, artifactType: Option[String], subType: Option[String], redirectToDownload: Option[Boolean]): Future[Either[Result, JsObject]] = {
    val func = "getPartnerLatestFirmware"

    val partnerId = agencyId.toString
    val artifactTypeUsed = artifactType.getOrElse(DeviceConstants.DefaultArtifactType)
    val subTypeUsed = subType.getOrElse(DeviceConstants.DefaultSubType)

    val request = GetPartnerLatestFirmwareDownloadRequest(partnerId = partnerId, deviceModel = deviceModel, `type` = Some(artifactTypeUsed), subType = Some(subTypeUsed))

    val serverError = errorResponse(Results.InternalServerError)
    val notFound = errorResponse(Results.NotFound)

    val downloadFileName = redirectToDownload match {
      case Some(true) => DeviceConstants.ModelToDownloadName get deviceModel
      case _ => None
    }

    val result = for {
      response <- FutureEither(arsenal.getPartnerLatestFirmware(request)).mapLeft(_ => serverError)
      firmware <- FutureEither.successful(response.firmware.toRight(notFound))
      version <- FutureEither.successful(firmware.displayVersion.toRight(notFound))
      transformedUrl <- transformUrl(func, partnerId, None, Some(version), response, downloadFileName)
    } yield firmwareDownloadToJson(deviceModel, artifactTypeUsed, subTypeUsed, version, transformedUrl)

    result.future
  }


  private def getIncludedJson(users: Seq[User], deviceHomes: Seq[DeviceHome], isV1: Boolean = true): Option[JsValue] = {
    def tryJsArray(jsValue: JsValue): Option[JsArray] = {
      jsValue match {
        case v: JsArray => Some(v)
        case _ => None
      }
    }

    val usersJson: Option[JsArray] =
      users match {
        case Nil => None
        case _ => tryJsArray(usersMinimalToJson(users, isV1))
      }
    val homesJson: Option[JsArray] =
      deviceHomes match {
        case Nil => None
        case _ => tryJsArray(deviceHomesJson(deviceHomes))
      }

    (usersJson, homesJson) match {
      case (Some(uJson), Some(hJson)) => Some(uJson ++ hJson)
      case (Some(uJson), None) => Some(uJson)
      case (None, Some(hJson)) => Some(hJson)
      case _ => None
    }
  }

  private def getOptionalFields(authenticatedRequest: JWTAuthenticatedRequest[AnyContent]): Option[Fields] = {
    val fields = authenticatedRequest.getQueryString("fields")

    fields.map(f => {
      val params = f.split(",").map(_.toLowerCase.trim).toSet

      Fields(
        assignedBy = Option(params("assignedby")),
        warranties = Option(params("warranties")),
        firmware = Option(params("firmware")),
        preferences = Option(params("preferences")),
        purchases = Option(params("purchases")),
        canary = Option(params("canary")),
        dateAssigned = Option(params("dateassigned")),
        errorInfo = Option(params("deviceerrors")),
        deviceHome = Option(params("devicehome")),
        lteInfo = Option(params("lteinfo"))
      )
    })
  }

  private def getOwnerFromRequest(agencyId: String, request: DeviceUpdateRequest): Option[EntityDescriptor] = {
    if (request.ownerId.isDefined && request.ownerType.isDefined) {
      Option(EntityDescriptor(JsonApiOrgToThriftV1.jsonToEntityType(request.ownerType.get).get, request.ownerId.get, agencyId))
    } else {
      None
    }
  }


  private[controllers] def getSearchParamsFromQuery(
    req: play.api.mvc.RequestHeader,
    agencyId: UUID,
    limitToOwner: Option[String],
    maybeOffset: Option[Int],
    maybeLimit: Option[Int]
  ): Either[Result, DeviceQueryParams] = {

    def checkExclusivity(mainParameter: Option[String], containsConflicts: Boolean) : Either[Result, Unit] = {
      if(mainParameter.nonEmpty && containsConflicts){
        Left(errorResponse(Results.BadRequest, detail = Some(s"Conflicting query parameters")))
      } else {
        Right(Unit)
      }
    }

    val includeReferenceEntities =
      req.getQueryString(SolrConstants.IncludeReferenceEntitiesString)
        .flatMap(s => Try(s.toBoolean).toOption)
        .getOrElse(false)

    for{
      lastUploadFrom <- Right(req.getQueryString("lastUploadFrom"))
      lastUploadTo <- Right(req.getQueryString("lastUploadTo"))
      functionalTestFrom <- Right(req.getQueryString("lastFunctionalTestFrom"))
      functionalTestTo <- Right(req.getQueryString("lastFunctionalTestTo"))
      // The following parameters cannot combined with last upload and functional test from/to parameters
      noFunctionalTestInDays <- Right(req.getQueryString("noFunctionalTestInDays"))
      notDockedInDays <- Right(req.getQueryString("notDockedInDays"))
      _ <- checkExclusivity(noFunctionalTestInDays, Seq(lastUploadFrom, lastUploadTo, functionalTestFrom, functionalTestTo, notDockedInDays).flatten.nonEmpty)
      _ <- checkExclusivity(notDockedInDays, Seq(lastUploadFrom, lastUploadTo, functionalTestFrom, functionalTestTo, noFunctionalTestInDays).flatten.nonEmpty)
    } yield {
      DeviceQueryParams(
        partnerId = agencyId,
        deviceFirmware = req.getQueryString(SolrConstants.DeviceFirmwareString).map(_.split(DeviceConstants.CommaChar).toSeq),
        deviceName = req.getQueryString(SolrConstants.NameString).filter(_.trim.nonEmpty),
        errorStatus = req.getQueryString("errorStatus"),
        facetPivot = req.getQueryString(SolrConstants.FacetPivotQueryString).map(FacetPivot.getFacetPivotFields),
        facets = req.getQueryString("facets").map(_.split(DeviceConstants.CommaChar).toSeq),
        filter = req.getQueryString("filter"),
        lastUploadFrom = lastUploadFrom,
        lastUploadTo = lastUploadTo,
        functionalTestFrom = functionalTestFrom,
        functionalTestTo = functionalTestTo,
        noFunctionalTestInDays = noFunctionalTestInDays,
        notDockedInDays = notDockedInDays,
        limit = maybeLimit,
        model = req.getQueryString(DeviceConstants.ModelString),
        offset = maybeOffset,
        ownerId = limitToOwner.orElse(req.getQueryString(DeviceConstants.OwnerIdString)),
        q = req.getQueryString("q"),
        serialNumber = req.getQueryString(DeviceConstants.SerialString).filter(_.trim.nonEmpty),
        sort = req.getQueryString("sort"),
        status = req.getQueryString(SolrConstants.StatusString),
        status2 = req.getQueryString(SolrConstants.Status2String),
        homeId = req.getQueryString("homeId"),
        ownerEvidenceGroup = req.getQueryString(DeviceConstants.OwnerEvidenceGroup),
        includeReferenceEntities = includeReferenceEntities
      )
    }
  }

  private def getSolrDevicesByScope(request: JWTAuthenticatedRequest[_], solrDevices: Seq[SolrDevice], agencyId: UUID): Seq[SolrDevice] = {
    solrDevices.flatMap(solrDevice => {
      solrDevice.owner.map(ed => {
        val (permissions, params) = getDeviceReadPermission(agencyId, ed.id)
        if (permissions.validate(request.jwt, params)) {
          solrDevice
        } else {
          solrDevice.copy(lastIssuedToTid = None, owner = None, updatedBy = None)
        }
      })
    })
  }

  private def getSolrResponse(
    deviceQueryParams: DeviceQueryParams,
    deviceSearchClient: DeviceSearchClient
  ): Future[Either[SearchError, SolrResponse]] = {
    val response = deviceSearchClient.search(deviceQueryParams, komrade)

    response
  }


  /**
    * @param agencyId - Agency Id
    * @param devices - List of arsenal thrift device metadata objects
    * @return - Either an error or a list of komrade users
    *
    * This function tries to consolidate all the user id's from the list of device metadata objects and then fetches user metadata list from komrade in a single thrift call
    */
  private def getUsersFromDeviceMetadataList(agencyId: UUID, devices: Seq[DeviceMetaData], isEdcaAudience: Boolean = false): Future[Either[Result, Seq[User]]] = {
    val deviceOwners: Seq[EntityDescriptor] = devices.flatMap(device => device.owner)
    val deviceUpdatedByEntities: Seq[EntityDescriptor] = devices.flatMap(device => device.updatedBy)
    val deviceAssignedByEntities: Seq[EntityDescriptor] =
      if (isEdcaAudience) devices.flatMap(device => device.assignedBy) else Seq()
    val contactIds: Seq[EntityDescriptor] =
      devices
        .flatMap(dv => dv.deviceHome)
        .flatMap(home => home.pointOfContactId)
        .flatMap { pointOfContactId =>
          if (StringUtils.isNullOrWhiteSpace(pointOfContactId)) {
            None
          } else {
            Some(EntityDescriptor(entityType = TidEntities.Subscriber, id = pointOfContactId, partnerId = agencyId.toString))
          }
        }

    val entityDescriptors = deviceOwners ++ deviceUpdatedByEntities ++ deviceAssignedByEntities ++ contactIds

    lookupUsers(entityDescriptors)
  }



  private def hasCewModifyPermission(agencyId: UUID, device: DeviceMetaData, authenticatedRequest: JWTAuthenticatedRequest[AnyContent]): Boolean = {
    val (permissions, params) = getCewModifyPermission(agencyId, device)
    agencyMatchesAuthenticatedUserAgency(agencyId.toString, authenticatedRequest) && permissions.validate(authenticatedRequest.jwt, params)
  }

  private def hasFleetDevicePermission(agencyId: UUID, authenticatedRequest: JWTAuthenticatedRequest[AnyContent]): Boolean = {
    val (permissions, params) = getFleetDevicePermission(agencyId)
    agencyMatchesAuthenticatedUserAgency(agencyId.toString, authenticatedRequest) && permissions.validate(authenticatedRequest.jwt, params)
  }

  private def hasModelBasedDeviceModifyPermission(agencyId: UUID, device: DeviceMetaData, authReq: JWTAuthenticatedRequest[AnyContent]): Boolean = {
    validateModelForPermissionCheck(device) {
      case model if DeviceConstants.CewDevicesModelPrefix.exists(model.startsWith) => hasCewModifyPermission(agencyId, device, authReq)
      case model if DeviceConstants.AirDevicesModelPrefix.exists(model.startsWith) => hasAirModifyPermission(agencyId, authReq) || hasDeviceModifyPermission(agencyId, device, authReq)
      case _ => hasDeviceModifyPermission(agencyId, device, authReq)
    }
  }

  private def hasRootDeviceExecutePermission(authenticatedRequest: JWTAuthenticatedRequest[AnyContent]): Boolean = {
    val params = Map(Permissions.partnerAccess -> Constants.rootPartnerUuid.toString)
    val permissions = Permissions(Scope(ECOMScopes.DEVICE_ANY_EXECUTE))
    permissions.validate(authenticatedRequest.jwt, params)
  }

  private def hasDeviceBulkTransferPermissions(authReq: JWTAuthenticatedRequest[AnyContent]): Boolean = {
    val permission = Permissions(And(Scope(ECOMScopes.DEVICE_ANY_READ), Scope(ECOMScopes.DEVICE_ANY_MODIFY)))
    permission.validate(authReq.jwt, Map.empty)
  }

  private def hasDeviceRegisterPermission(agencyId: UUID, device: DeviceMetaData, authReq: JWTAuthenticatedRequest[AnyContent]): Boolean = {
    validateModelForPermissionCheck(device) {
      case model if DeviceConstants.CewDevicesModelPrefix.exists(model.startsWith) => hasCewModifyPermissionAgency(agencyId, authReq)
      case model if DeviceConstants.AirDevicesModelPrefix.exists(model.startsWith) => hasAirModifyPermission(agencyId, authReq) || hasDeviceModifyPermission(agencyId, device, authReq)
      case _ => hasDeviceModifyPermission(agencyId, device, authReq) || hasFleetDevicePermission(agencyId, authReq)
    }
  }

  private def hasPermissionsToGetDashboardInfo(
    model: String,
    authenticatedRequest: JWTAuthenticatedRequest[AnyContent],
    agencyId: UUID,
    forbiddenErrorDetail: String
  ): Either[Result, Unit] = {
    if (hasModelBasedDeviceModifyPermissionAgency(agencyId, model, authenticatedRequest) && hasDeviceListPermissionAgency(agencyId, authenticatedRequest)) {
      Right(())
    } else {
      Left(errorResponse(resultStatus = Results.Forbidden, detail = Some(forbiddenErrorDetail)))
    }
  }

  private def hasEvidenceOffloadPermission(agencyId: UUID, authenticatedRequest: JWTAuthenticatedRequest[AnyContent]): Boolean = {
    val (permissions, params) = getEvidenceOffloadPermission(agencyId)
    agencyMatchesAuthenticatedUserAgency(agencyId.toString, authenticatedRequest) && permissions.validate(authenticatedRequest.jwt, params)
  }

  private def listDevices(agencyId: UUID, deviceIds: Seq[String]): Future[Either[Result, ListDeviceResponse]] = {
    val listRequest = ListDeviceRequest(partnerId = agencyId.toString, deviceIds = Some(deviceIds))
    val listDevicesPromise = arsenal.listDevices(listRequest)
    val listDevicesResponse: Future[Either[Result, ListDeviceResponse]] =
      for (resp <- listDevicesPromise) yield {
        resp match {
          case Right(r) => Right(r)
          case Left(err) => Left(arsenalErrorToResponse(err))
        }
      }

    listDevicesResponse
  }

  private def lookUpDeviceHomes(agencyId: UUID, deviceHomeIds: Seq[UUID]): Future[Either[Result, Seq[DeviceHome]]] = {
    val result =
      if (deviceHomeIds.nonEmpty) {
        val request = ListDeviceHomesRequest(partnerId = agencyId.toString, deviceHomeIds = Some(deviceHomeIds.map(_.toString)))
        arsenal.listDeviceHomes(request).map {
          case Left(errorCode) => Left(arsenalErrorToResponse(errorCode))
          case Right(response) => Right(response.deviceHomes.getOrElse(Nil))
        }
      } else {
        Future.successful(Right(Nil))
      }

    result
  }

  /*
  * Look up user information by Tid. Does not allow lookup for root partner/domain.
  */
  private def lookupUsers(entityDescriptors: Seq[EntityDescriptor]): Future[Either[Result, Seq[User]]] = {
    val func = "lookupUsers"

    val users =
      entityDescriptors
      .filter(e => e.entityType == TidEntities.Subscriber && Convert.fromGuidToTidFormat(e.partnerId) != rootPartnerIdTidFormat)
      .map(t => KomradeEntityDescriptor(TidEntities.Subscriber, t.id, t.partnerId))

    KomradeUtils.lookupUsersExceedingKomradeLimit(users, komrade).map {
      case Left(error) => {
        logger.error("Error fetching users from komrade")(DeviceConstants.FuncString -> func, "headerStatus" -> error.header.status)
        Left(errorResponse(resultStatus = Results.InternalServerError, detail = Some("Error fetching users from komrade")))
      }
      case Right(users) => Right(users)
    }
  }

  private def parseDeviceIdsFromRequest(payload: JsValue): Seq[String] = {
    val deviceIds: Option[Seq[String]] =
      for {
        devicesJson <- (payload \ dataField).asOpt[Seq[JsValue]]
      } yield {
        devicesJson.flatMap { dJson =>
          (dJson \ idField).asOpt[String]
        }
      }

    deviceIds.getOrElse(Nil)
  }

  private def parseDeviceUpdateRequest(json: JsValue, id: Option[String] = None): DeviceUpdateRequest = {
    val serial = (json \ DeviceConstants.DataString \ DeviceConstants.AttributesString \ DeviceConstants.SerialString).asOpt[String]
    val model = (json \ DeviceConstants.DataString \ DeviceConstants.AttributesString \ DeviceConstants.ModelString).asOpt[String]
    val make = (json \ DeviceConstants.DataString \ DeviceConstants.AttributesString \ "make").asOpt[String]
    val friendlyName = (json \ DeviceConstants.DataString \ DeviceConstants.AttributesString \ "friendlyName").asOpt[String]
    val ownerId = (json \ DeviceConstants.DataString \ DeviceConstants.RelationshipsString \ DeviceConstants.OwnerString \ DeviceConstants.DataString \ DeviceConstants.IdString).asOpt[String]
    val ownerType = (json \ DeviceConstants.DataString \ DeviceConstants.RelationshipsString \ DeviceConstants.OwnerString \ DeviceConstants.DataString \ "type").asOpt[String]
    val prefs = (json \ DeviceConstants.DataString \ DeviceConstants.AttributesString \ "preferences").asOpt[Seq[JsValue]]
    val dateLastUpload = (json \ DeviceConstants.DataString \ DeviceConstants.AttributesString \ "lastUploadDateTime").asOpt[String]
    val preferences = prefs.map(p => p.flatMap(fromPreferencesJson))

    DeviceUpdateRequest(id, serial, model, make, friendlyName, ownerId, ownerType, preferences, dateLastUpload)
  }

  private def parseBulkDeviceTransferRequest(json: JsValue): EdcaBulkDeviceTransferRequest = {
    val currentAgency = (json \ "currentAgency").asOpt[String]
    val targetAgency = (json \ "targetAgency").asOpt[String]
    val model = (json \ "model").asOpt[String]
    val deviceIds = (json \ "deviceIds").asOpt[Seq[String]].getOrElse(Nil)
    EdcaBulkDeviceTransferRequest(currentAgency, targetAgency, model, deviceIds)
  }

  private def parseSendAdminCommandsRequest(json: JsValue): EdcaSendAdminCommandsRequest = {
    val command = (json \ "command").asOpt[String]
    val serial = (json \ "serial").asOpt[String]
    val partnerId = (json \ "partnerId").asOpt[String]
    val model = (json \ "model").asOpt[String]
    val expirationEpochSeconds = (json \ "expirationEpochSeconds").asOpt[Long]
    val idempotencyKey = (json \ "idempotencyKey").asOpt[String]
    val action = (json \ "action").asOpt[String]
    EdcaSendAdminCommandsRequest(command, serial, model, partnerId, expirationEpochSeconds, idempotencyKey, action)
  }

  private def processDeviceSearch(
    agencyId: UUID,
    authenticatedRequest: JWTAuthenticatedRequest[AnyContent],
    deviceQueryParams: DeviceQueryParams,
    deviceSearchClient: DeviceSearchClient,
    attachAdditionalData: Boolean,
  ): Future[Either[Result, JsObject]] = {

    def getDeviceSearchSolrResponse(): Future[Either[Result, SolrResponse]] = {
      getSolrResponse(deviceQueryParams, deviceSearchClient).map {
        case Left(searchError) => {
          searchError match {
            case BadSearchRequest(description) => {
              logger.error("searchRequestError")(DeviceConstants.ErrorString -> description)
              Left(errorResponse(resultStatus = Results.BadRequest, detail = Some(description)))
            }
            case SolrError(description) => {
              logger.error("searchEngineError")(DeviceConstants.ErrorString -> description)
              Left(errorResponse(resultStatus = Results.InternalServerError, detail = Some(description)))
            }
            case DependencyError(response) =>
              val description = response.body.toString
              logger.error("searchDependencyError")(DeviceConstants.ErrorString -> description)
              Left(errorResponse(resultStatus = Results.InternalServerError, detail = Some(description)))
          }
        }
        case Right(resp) => Right(resp)
      }
    }

    val result =
      for {
        solrResponse <- FutureEither(getDeviceSearchSolrResponse())
        json <- FutureEither(if (attachAdditionalData) processSolrResponseDeviceSearch(solrResponse, agencyId, authenticatedRequest, deviceQueryParams)
                      else processSolrResponseDeviceSearchWithoutAdditionalData(solrResponse, agencyId, authenticatedRequest, deviceQueryParams))
      } yield {
        json
      }

    result.fold(l => Left(l), r => Right(r))
  }

  private def sendEvidenceOffloadRequest(agencyId: String, deviceId: String, evidenceId: String, requestType: EvidenceOffloadRequestType): Future[Either[Result, JsValue]] = {
    val func = "sendEvidenceOffloadRequest"
    val logArgs = List(
      DeviceConstants.AgencyIdString -> agencyId,
      DeviceConstants.DeviceIdString -> deviceId,
      DeviceConstants.EvidenceIdString -> evidenceId,
      DeviceConstants.RequestTypeString -> EvidenceOffloadRequestType
    )
    logger.debug(func)(logArgs: _*)
    val request = EvidenceOffloadRequest(requestType, agencyId, deviceId, evidenceId)
    val requestInfo = RequestInfo.toThrift(RequestInfo.newRequestInfo())
    sherlock.sendEvidenceOffloadRequestToConnectedDevice(request, requestInfo).map {
      case Left(e) =>
        logger.error("SendEvidenceOffloadRequestFailure")((("error" -> e) :: logArgs): _*)
        Left(e)
      case Right(response) =>
        val logArgsWithResponseCode = ("responseCode" -> response.code.name) :: logArgs
        response.code match {
          case Transmitted =>
            logger.debug("SendEvidenceOffloadRequestSuccess")(logArgsWithResponseCode: _*)
            Right(Json.obj(dataField -> "Transmitted"))
          case ConnectionNotFound =>
            logger.error("SendEvidenceOffloadRequestFailure")(logArgsWithResponseCode: _*)
            Left(errorResponse(Results.NotFound))
          case _ =>
            logger.error("SendEvidenceOffloadRequestFailure")(logArgsWithResponseCode: _*)
            Left(errorResponse(Results.InternalServerError))
        }
    }
  }

  private def validateAirDeviceReadPermission(agencyId: UUID, authenticatedRequest: JWTAuthenticatedRequest[AnyContent]): Either[Result, Unit] = {
    val (permissions, params) = getAirDeviceReadPermission(agencyId)
    if (permissions.validate(authenticatedRequest.jwt, params)) {
      Right((): Unit)
    } else {
      Left(errorResponse(Results.Forbidden))
    }
  }

  private def processQueryResponse(authenticatedRequest: JWTAuthenticatedRequest[AnyContent], logs: Seq[DeviceLogMetadata]): Result = {
    Ok(
      Json.obj(
        dataField -> deviceLogInfoToJsonArray(logs)))
  }

  private def processSolrResponseDeviceSearch(
    solrResponse: SolrResponse,
    agencyId: UUID,
    authenticatedRequest: JWTAuthenticatedRequest[AnyContent],
    deviceQueryParams: DeviceQueryParams,
  ): Future[Either[Result, JsObject]] = {
    val func = "processSolrResponseDeviceSearch"

    def getUsersToLookup(solrDevices: Seq[SolrDevice], deviceHomes: Seq[DeviceHome]): Seq[EntityDescriptor] = {
      val ownerEds = solrDevices.flatMap(_.owner)
      val updatedByEds = solrDevices.flatMap(_.updatedBy)
      val lastIssuedToEds = solrDevices.flatMap(_.lastIssuedToTid)

      val pointOfContactIds = deviceHomes.flatMap(_.pointOfContactId)
      val pointOfContactEds = pointOfContactIds.map { contactId =>
        EntityDescriptor(entityType = TidEntities.Subscriber, id = contactId, partnerId = agencyId.toString)
      }

      ownerEds ++ updatedByEds ++ lastIssuedToEds ++ pointOfContactEds
    }

    def getGroupsToLookup(solrDevices: Seq[SolrDevice]) = {
      val teamIds: Set[TeamId] = solrDevices.flatMap(device =>
        device.ownerEvidenceGroup.flatMap(groupIdToTeamId(_, device.partnerId))
      ).toSet
      teamProvider.getTeams(teamIds, agencyId)
    }
    //  isV1 is to preserve the partner API route as changes need to be made to this function.
    //  This should not be the pattern for future major route changes.
    val isV1 = authenticatedRequest.path.contains("/api/v1/")
    val result =
      for {
        solrDevices <- FutureEither.successful{
          Right(getSolrDevices(solrResponse))
        }

        solrDevicesByScope <- FutureEither.successful {
          val modelBlacklist = DeviceConstants.SnfFilterSet
          Right(getSolrDevicesByScope(authenticatedRequest, solrDevices, agencyId).filter(d => !modelBlacklist.contains(d.model.getOrElse(""))))
        }

        deviceHomes <- FutureEither {
          val homeIds = solrDevices.flatMap(_.homeId)
          lookUpDeviceHomes(agencyId, homeIds)
        }
        groups <- FutureEither{
          getGroupsToLookup(solrDevices)

        }
        users <- FutureEither {
          lookupUsers(getUsersToLookup(solrDevices, deviceHomes))
        }

        jsonResponse <- FutureEither.successful {
          val groupMap = groups.map(group => group.team.id -> group).toMap
          val devicesJson = solrDeviceToJson(solrResponse, solrDevicesByScope, groupMap, isV1)
          val json = Json.obj(
            metaField -> Json.obj(
              offsetField -> solrResponse.start,
              limitField -> solrResponse.docs.size,
              countField -> solrResponse.numFound,
              userCountField -> users.size
            ),
            dataField -> devicesJson
          )

          Right(json)
        }

        jsonResponseWithIncluded <- FutureEither.successful {
          // If performing a device search from EDCA we should not include extra data when looking at any other agency
          // Some of this may be personal details that shouldn't be exposed
          if (hasRootAgencyAccessPermission(authenticatedRequest) && !agencyMatchesAuthenticatedUserAgency(agencyId.toString, authenticatedRequest)) {
            Right(jsonResponse)
          } else {
            getIncludedJson(users, deviceHomes, isV1) match {
              case Some(json) => Right(jsonResponse ++ Json.obj(includedField -> json))
              case None => Right(jsonResponse)
            }
          }
        }

        jsonResponseWithFacets <- FutureEither.successful {
          deviceQueryParams.facetPivot match {
            case Some(_) => getFacetPivotsJson(solrResponse, jsonResponseWithIncluded, func)
            case None => Right(jsonResponseWithIncluded)
          }
        }
      } yield {
        jsonResponseWithFacets
      }

    result.fold(l => Left(l), r => Right(r))
  }

  private def getFacetPivotsJson(solrResponse: SolrResponse, jsonResponse: JsObject, func: String): Either[Result, JsObject] = {
    Solr.getFacetPivotsJson(solrResponse.resp) match {
      case Success(facetPivotJson) => {
        val json = jsonResponse ++ facetPivotJson
        Right(json)
      }
      case Failure(exception) => {
        val errorMessage = s"Problem with '${SolrConstants.FacetPivotResponseString}' processing"
        logger.error(exception, func)(DeviceConstants.ErrorString -> errorMessage)
        val errResponse = errorResponse(Results.InternalServerError, detail = Some(errorMessage))
        Left(errResponse)
      }
    }
  }

  private def processSolrResponseDeviceSearchWithoutAdditionalData(
    solrResponse: SolrResponse,
    agencyId: UUID,
    authenticatedRequest: JWTAuthenticatedRequest[AnyContent],
    deviceQueryParams: DeviceQueryParams,
  ): Future[Either[Result, JsObject]] = {
    val func = "processSolrResponseDeviceSearchWithoutAdditionalData"

    Try {
      val solrDevices = getSolrDevices(solrResponse)
      val solrDevicesByScope =
        getSolrDevicesByScope(authenticatedRequest, solrDevices, agencyId).
          filter(d => !DeviceConstants.SnfFilterSet.contains(d.model.getOrElse("")))
      val devicesJson = solrDeviceToJson(solrResponse, solrDevicesByScope)
      val jsonResponse = Json.obj(
        metaField -> Json.obj(
          offsetField -> solrResponse.start,
          limitField -> solrResponse.docs.size,
          countField -> solrResponse.numFound,
        ),
        dataField -> devicesJson
      )
      deviceQueryParams.facetPivot match {
        case Some(_) => getFacetPivotsJson(solrResponse, jsonResponse, func)
        case None => Right(jsonResponse)
      }
    } match {
      case Failure(ex) =>
        logger.error(ex, func)("partnerId" -> agencyId)
        Future.successful(Left(errorResponse(Results.InternalServerError)))
      case Success(either) => Future.successful(either)
    }
  }

  private def processSolrResponseFirmware(
    solrResponse: SolrResponse
  ): Either[Result, GetAgencyFirmwareVersionsResponse] = {
    val tryFacetPivot = Solr.getFacetPivots(solrResponse.resp)
    val total = solrResponse.numFound.getOrElse(0)

    val errorResultOrResponse =
      tryFacetPivot match {
        case Success(facetPivot) => {
          val agencyFirmwareVersions = facetPivot.list.map(pivot => pivot.value -> pivot.count).toMap
          val getAgencyFirmwareVersionsResponse = GetAgencyFirmwareVersionsResponse(total, agencyFirmwareVersions)
          Right(getAgencyFirmwareVersionsResponse)
        }

        case Failure(exception) => {
          val errResponse =
            errorResponse(
              Results.InternalServerError,
              detail = Some(s"Problem with '${SolrConstants.FacetPivotResponseString}' processing")
            )

          Left(errResponse)
        }
      }

    errorResultOrResponse
  }

  private def registerDevice(
    authenticatedRequest: JWTAuthenticatedRequest[JsValue],
    registerDeviceRequest: RegisterDeviceRequest
  ): Future[Either[Result, DeviceMetaData]] = {
    val func = "registerDevice"
    logger.debug(func)()
    registerDevice(registerDeviceRequest)
  }

  private def registerDevice(registerDeviceRequest: RegisterDeviceRequest): Future[Either[Result, DeviceMetaData]] = {
    arsenal.registerDevice(registerDeviceRequest).map {
      case Left(err) => Left(arsenalErrorToResponse(err))
      case Right(resp) => resp.device.toRight(Results.InternalServerError)
    }
  }

  private def processDockBasedRegistration(jwt: DeviceJWT, partnerId: String): FutureEither[Result, DeviceMetaData] = {
    def setDockBasedRegistationTime(partnerId: String, dockId: String): Future[Either[Result, Unit]] = {
      val request = SetEntityBasedRegisterTimeRequest(partnerId, dockId, System.currentTimeMillis())
      arsenal.setEntityBasedRegisterTime(request).map {
        case Left(err) => Left(arsenalErrorToResponse(err))
        case Right(_) => Right(Unit)
      }
    }

    def getDockIfRegisteredToPartner(partnerId: String, dockSerial: String): Future[Either[Result, DeviceMetaData]] = {
      val request = GetDeviceBySerialQuery(partnerId, dockSerial)
      arsenal.getDeviceBySerial(request).map {
        case Right(r) => r.device.toRight(errorResponse(Results.Forbidden))
        case Left(l) => Left(arsenalErrorToResponse(l))
      }
    }

    for {
      dockMetadata <- FutureEither(getDockIfRegisteredToPartner(partnerId, jwt.dockSerial.get))
      _ <- FutureEither(Future.successful(Either.cond(allowedDockBasedRegisterStatus.contains(dockMetadata.status2.get), Unit, Results.Forbidden)))
      metadata <- {
        val request = RegisterDeviceRequest(partnerId
          , jwt.serialNumber.value
          , jwt.model.value
          , partnerEntityDescriptor(rootPartnerIdTidFormat)
          , forceMoveDeviceFromAnotherAgency = Some(false))
        FutureEither(registerDevice(request))
      }
      _ <- FutureEither(setDockBasedRegistationTime(partnerId, dockMetadata.id.get))
    } yield {
      metadata
    }
  }

  private def removeDeviceHomeFromDevicesImpl(agencyId: UUID, devices: Seq[DeviceWithHttpStatusMetadata], updatedBy: EntityDescriptor): Future[Either[Result, Seq[DeviceWithErrorCode]]] = {
    if (devices.nonEmpty) {
      def arsenalResponseForRemoveDeviceHome(): Future[Either[Result, RemoveDeviceHomeResponse]] = {
        val request = RemoveDeviceHomeRequest(partnerId = agencyId.toString, deviceIds = devices.map(_.id), updatedBy = updatedBy)
        arsenal.removeDeviceHome(request).map {
          case Left(error) => Left(arsenalErrorToResponse(error))
          case Right(resp) => Right(resp)
        }
      }

      def getDeviceMetadataListFromArsenalResponse(arsenalResponse: RemoveDeviceHomeResponse): Future[Either[Result, Seq[DeviceMetaData]]] = {
        Future.successful(
          arsenalResponse.devices
            .filter(_.nonEmpty)
            .toRight(
              errorResponse(resultStatus = Results.InternalServerError, detail = Some("Device metadata missing in remove device home response"))
            )
        )
      }

      val result: FutureEither[Result, Seq[DeviceWithErrorCode]] =
        for {
          arsenalResponse <- FutureEither(arsenalResponseForRemoveDeviceHome())
          deviceMetadataList <- FutureEither(getDeviceMetadataListFromArsenalResponse(arsenalResponse))
        } yield {
          getDevicesWithErrorCode(deviceMetadataList)
        }

      result.fold(l => Left(l), r => Right(r))
    } else {
      Future.successful(Right(Nil))
    }
  }

  private def searchLogsByDevice(authenticatedRequest: JWTAuthenticatedRequest[AnyContent]) = {
    val func = "searchLogsByDevice"
    logger.debug(func)()

    val deviceModel = authenticatedRequest.getQueryString(LogConstants.DeviceModelQueryParam)
    val serial = authenticatedRequest.getQueryString(LogConstants.SerialQueryParam)
    var startDate = authenticatedRequest.getQueryString(LogConstants.StartDateQueryParam)
    var endDate = authenticatedRequest.getQueryString(LogConstants.EndDateQueryParam)
    val nowTime = ZonedDateTime.now(ZoneOffset.UTC)

    // Default to most recent 30 days if not supplied.
    if (startDate.isEmpty || startDate.get.isEmpty) {
      startDate = Some(nowTime.minusDays(30).format(DateTimeFormatter.ISO_INSTANT))
    }

    if (endDate.isEmpty || endDate.get.isEmpty) {
      endDate = Some(nowTime.format(DateTimeFormatter.ISO_INSTANT))
    }

    if (serial.isDefined && deviceModel.isDefined && startDate.isDefined && endDate.isDefined) {
      filterModelsFromResultFuture(deviceModel, DeviceConstants.SnfFilterSet) {
        val queryRequest = QueryLogsByDeviceRequest(startDate.get, endDate.get, serial.get, deviceModel.get)
        val promise = arsenal.queryLogsByDevice(queryRequest)
        for {resp <- promise} yield {
          resp match {
            case Left(error) => arsenalErrorToResponse(error)
            case Right(searchResponse) =>
              searchResponse.logs.map(logs => processQueryResponse(authenticatedRequest, logs))
              .getOrElse(getEmptyListResponse(authenticatedRequest.request.uri))
          }
        }
      }
    } else {
      Future.successful(errorResponse(Results.BadRequest))
    }
  }

  private def searchLogsByFileKey(authenticatedRequest: JWTAuthenticatedRequest[AnyContent]) = {
    val func = "searchLogsByFileKey"
    logger.debug(func)()

    authenticatedRequest.getQueryString(LogConstants.FileKeyQueryParam) match {
      case Some(fileKey) =>
        val queryRequest = QueryLogByFileKeyRequest(fileKey)
        arsenal.queryLogByFileKey(queryRequest).map {
          case Left(error) => arsenalErrorToResponse(error)
          case Right(searchResponse) =>
            searchResponse.logs.map(log => processQueryResponse(authenticatedRequest, log)).getOrElse(getEmptyListResponse(authenticatedRequest.request.uri))
        }
      case None => Future.successful(errorResponse(Results.BadRequest))
    }
  }

  private def searchLogsByPartner(authenticatedRequest: JWTAuthenticatedRequest[AnyContent]) = {
    val func = "searchLogsByPartner"
    logger.debug(func)()

    val deviceModel = authenticatedRequest.getQueryString(LogConstants.DeviceModelQueryParam)
    val agencyId = authenticatedRequest.getQueryString(LogConstants.AgencyIdQueryParam)
    val startDate = authenticatedRequest.getQueryString(LogConstants.StartDateQueryParam)
    val endDate = authenticatedRequest.getQueryString(LogConstants.EndDateQueryParam)

    if (agencyId.isDefined && deviceModel.isDefined && startDate.isDefined && endDate.isDefined) {
      filterModelsFromResultFuture(deviceModel, DeviceConstants.SnfFilterSet) {
        val queryRequest = QueryLogsByPartnerRequest(startDate.get, endDate.get, agencyId.get, deviceModel.get)
        val promise = arsenal.queryLogsByPartner(queryRequest)
        for {resp <- promise} yield {
          resp match {
            case Left(error) => arsenalErrorToResponse(error)
            case Right(searchResponse) =>
              searchResponse.logs.map(logs => processQueryResponse(authenticatedRequest, logs))
              .getOrElse(getEmptyListResponse(authenticatedRequest.request.uri))
          }
        }
      }
    } else {
      Future.successful(errorResponse(Results.BadRequest))
    }
  }

  private def setDeviceHomeImpl(agencyId: String, deviceHomeId: Option[UUID], authenticatedRequest: JWTAuthenticatedRequest[JsValue]): FutureEither[Result, Result] = {
    val func = "setDeviceHomeImpl"
    logger.debug(func)(DeviceConstants.FuncString -> func, DeviceConstants.AgencyIdString -> agencyId)

    val updatedBy = updatedByFromJwt(authenticatedRequest.jwt)

    val result =
      for {
        agencyUuid <- FutureEither.successful(validateAgencyId(agencyId))
        deviceUuids <- FutureEither.successful(validateDeviceIdsFromRequestPayload(authenticatedRequest.request.body))
        // check to see if the requesting entity has permission to modify device metadata
        devicesWithHttpStatusMetadata <- {
          def permissionValidation(deviceMetaData: Option[DeviceMetaData]): Either[Result, Unit] =
            validatePermissionToModifyDevice(agencyUuid, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]], deviceMetaData)
          FutureEither(validatePermissionToModifyDeviceMetadata(agencyUuid, authenticatedRequest, deviceUuids.map(_.toString), permissionValidation))
        }

        prohibitedDevices <- FutureEither.successful(Right(devicesWithHttpStatusMetadata.filter(_.httpStatusMetadata.resultStatus != Results.Ok)))
        devicesToUpdate <- FutureEither.successful(Right(devicesWithHttpStatusMetadata.filter(_.httpStatusMetadata.resultStatus == Results.Ok)))
        devicesWithErrorFromResponse <- FutureEither {
          deviceHomeId match {
            case Some(homeId) => assignDeviceHomeImpl(agencyId = agencyUuid, homeId = homeId, devices = devicesToUpdate, updatedBy = updatedBy)
            case None => removeDeviceHomeFromDevicesImpl(agencyId = agencyUuid, devices = devicesToUpdate, updatedBy = updatedBy)
          }
        }
        users <- FutureEither(getUsersFromDeviceMetadataList(agencyUuid, devicesWithErrorFromResponse.flatMap(_.device)))
        deviceHomes <- FutureEither.successful(Right(devicesWithErrorFromResponse.flatMap(_.device).flatMap(_.deviceHome)))
      } yield {
        val includedJson = getIncludedJson(users, deviceHomes.headOption.toSeq)
        Ok(deviceWithErrorListJsonResponse(devicesWithErrorFromResponse, prohibitedDevices, includedJson))
      }

    result
  }

  private def setDeviceName(
    authenticatedRequest: JWTAuthenticatedRequest[JsValue],
    agencyId: String,
    id: String,
    request: DeviceUpdateRequest,
    updatedBy: EntityDescriptor
  ): Future[Either[Result, SetDeviceNameResponse]] = {
    val func = "setDeviceName"
    logger.debug(func)()
    request.friendlyName.map { friendlyName =>
      val renameRequest = SetDeviceNameRequest(agencyId, id, updatedBy, friendlyName)

      arsenal.setDeviceName(renameRequest).map {
        case Left(err) => Left(arsenalErrorToResponse(err))
        case Right(resp) => Right(resp)
      }
    }.getOrElse(Future.successful(Right(SetDeviceNameResponse(None))))
  }

  private def setDevicePrefs(
    authenticatedRequest: JWTAuthenticatedRequest[JsValue],
    agencyId: String,
    id: String,
    request: DeviceUpdateRequest,
    updatedBy: EntityDescriptor
  ): Future[Either[Result, SetDevicePreferencesResponse]] = {
    val func = "setDevicePrefs"
    logger.debug(func)()

    request.preferences.map { _ =>
      val setPrefReq = SetDevicePreferencesRequest(agencyId, id, request.preferences.get, updatedBy)
      arsenal.setDevicePreferences(setPrefReq).map {
        case Left(err) => Left(arsenalErrorToResponse(err))
        case Right(resp) => Right(resp)
      }
    }.getOrElse(Future.successful(Right(SetDevicePreferencesResponse(None))))
  }



  private def toCanaryDevicesWithUsers(devices: Seq[DeviceMetaData], komradeTids: Seq[KomradeEntityDescriptor]): Future[Result] = {
    val usersPromise = komrade.getUserSeq(komradeTids)

    for (resp <- usersPromise) yield {
      resp match {
        case Left(result: Result) => result
        case Right(users: Seq[User]) => Ok(deviceListJsonResponse(devices, usersMinimalToJson(users)));
      }
    }
  }

  private def toKomradeEntityDescriptor(ent: EntityDescriptor): KomradeEntityDescriptor = {
    KomradeEntityDescriptor(ent.entityType, ent.id, ent.partnerId)
  }

  private def updateDeviceOwnerAndName(
    agencyId: UUID,
    deviceResponse: GetDeviceResponse,
    request: DeviceUpdateRequest,
    authenticatedRequest: JWTAuthenticatedRequest[JsValue],
    id: String,
    updatedBy: EntityDescriptor): FutureEither[Result, (AssignDeviceResponse, SetDeviceNameResponse)] = {

    if (request.ownerId.isDefined || request.friendlyName.isDefined) {
      for {
        _ <- FutureEither.successful(validatePermissionToModifyDeviceAgency(agencyId, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]], deviceResponse.device))
        assignDeviceResponse <- FutureEither(assignDevice(authenticatedRequest, agencyId.toString, id, request, updatedBy))
        renameDeviceResponse <- FutureEither(setDeviceName(authenticatedRequest, agencyId.toString, id, request, updatedBy))
      } yield (assignDeviceResponse, renameDeviceResponse)
    } else {
      FutureEither.successful(Right((AssignDeviceResponse(None), SetDeviceNameResponse(None))))
    }
  }

  private def updateDevicePreferences(
    agencyId: UUID,
    deviceResponse: GetDeviceResponse,
    request: DeviceUpdateRequest,
    authenticatedRequest: JWTAuthenticatedRequest[JsValue],
    id: String,
    updatedBy: EntityDescriptor): FutureEither[Result, SetDevicePreferencesResponse] = {

    if (request.preferences.isDefined) {
      for {
        _ <- FutureEither.successful(validatePermissionToModifyDevice(agencyId, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]], deviceResponse.device))
        setDevicePrefsResponse <- FutureEither(setDevicePrefs(authenticatedRequest, agencyId.toString, id, request, updatedBy))
      } yield setDevicePrefsResponse
    } else {
      FutureEither.successful(Right(SetDevicePreferencesResponse(None)))
    }
  }


  private def updateResponsesToDevice(
    assignDeviceResponse: AssignDeviceResponse,
    setDevicePrefsResponse: SetDevicePreferencesResponse,
    setDeviceNameResponse: SetDeviceNameResponse,
    agencyId: UUID,
    id: String
  ): Future[Either[Result, DeviceMetaData]] = {
    val responses = Seq(assignDeviceResponse.device, setDevicePrefsResponse.device, setDeviceNameResponse.device).flatten

    if (responses.nonEmpty) {
      val fields = Fields(assignedBy = Some(true), preferences = Some(true), dateAssigned = Some(true))

      val promise: Future[Either[ServiceErrorCode, GetDeviceResponse]] = getResponseForGetDeviceRequest(agencyId, GetDeviceById(id), Some(fields))

      for (resp <- promise) yield {
        resp match {
          case Left(err) => Left(arsenalErrorToResponse(err))
          case Right(r) =>
            r.device.map(d => Right(d))
            .getOrElse(Left(NotFound(Json.obj(errorsField -> DeviceConstants.DeviceNotFoundErrorMessage))))
        }
      }
    } else {
      Future.successful(Left(errorResponse(Results.BadRequest)))
    }
  }

  private def usersMinimalToJson(users: Seq[User], isV1: Boolean = true): JsValue = {
    val userWriteConverter =
      if (!isV1) userWritesMinimalV2
      else userWritesMinimal
    Json.toJson(users)(Writes.seq(userWriteConverter))
  }



  private def validateCreateRequest(params: DeviceUpdateRequest): Option[DeviceUpdateRequest] = {
    val invalid =
      params.serial.isEmpty || params.model.isEmpty ||
      (params.friendlyName.isDefined && params.friendlyName.get.length > 256) ||
      (params.ownerType.isDefined && params.ownerId.isEmpty) ||
      (params.ownerType.isEmpty && params.ownerId.isDefined) ||
      (params.ownerType.isDefined && JsonApiOrgToThriftV1.jsonToEntityType(params.ownerType.get).isEmpty) ||
      // If ownerId is present, ensure that it is a proper UUID
      (params.ownerId.isDefined && Convert.tryToUuid(params.ownerId.get).isEmpty) ||
      params.model.contains(AxonSnfServer.toString) ||
      params.dateLastUpload.map(d => Try(ZonedDateTime.parse(d)).isFailure).getOrElse(false)

    if (invalid) {
      None
    } else {
      Some(params)
    }
  }


  /**
    * @param payload - JSON payload in request body
    * @return - Either an error or list of device uuid's
    *
    * NOTE: This method does the following,
    * 1. parses the list of deviceIds from the JSON payload
    * 2. Creates a mapping between the device id and the UUID
    * 3. Checks to see if there is a device id that is not a valid UUID
    * 4. Checks to see if there is a duplicate device id in the request
    */
  private def validateDeviceIdsFromRequestPayload(payload: JsValue): Either[Result, Seq[UUID]] = {
    val deviceIds = parseDeviceIdsFromRequest(payload)
    val deviceUuidsMap: Map[String, Option[UUID]] =
      deviceIds
        .map(id => id -> Convert.tryToUuid(id))
        .toMap

    val deviceIdInvalid: Option[String] =
      deviceUuidsMap
        .find { case (_, maybeUuid) => maybeUuid.isEmpty }
        .toMap.keySet
        .headOption

    val duplicateDeviceId =
      deviceIds
        .map(_.toLowerCase)
        .groupBy(id => id)
        .find { case (_, list) => list.size > 1 }
        .toMap.keySet
        .headOption

    if (deviceIds.isEmpty) {
      Left(errorResponse(resultStatus = Results.BadRequest, detail = Some("DeviceId's missing in the request")))
    } else if (deviceIdInvalid.nonEmpty) {
      Left(errorResponse(resultStatus = Results.BadRequest, detail = Some(s"DeviceId '${deviceIdInvalid.getOrElse("")}' is not a valid UUID")))
    } else if (duplicateDeviceId.nonEmpty) {
      Left(errorResponse(resultStatus = Results.BadRequest, detail = Some(s"Duplicate deviceId, '${duplicateDeviceId.getOrElse("")}' present in the request")))
    } else {
      val deviceUuids = deviceUuidsMap.values.flatten.toSeq
      Right(deviceUuids)
    }
  }

  private def validateDeviceStatusUpdateRequest(payload: JsValue, newStatus: Option[String]): Either[Result, DeviceStatusUpdateRequest] = {
    validateDeviceIdsFromRequestPayload(payload).flatMap { deviceIds =>
      val newDeviceStatus = newStatus.flatMap(DeviceStatus2.valueOf).getOrElse(DeviceStatus2.Unknown)

      if (newDeviceStatus == DeviceStatus2.Unknown) {
        Left(errorResponse(resultStatus = Results.BadRequest, detail = Some("New device status is invalid")))
      } else {
        Right(
          DeviceStatusUpdateRequest(deviceIds.map(_.toString), newDeviceStatus)
        )
      }
    }
  }

  private def validateModelForPermissionCheck(device: DeviceMetaData)(body: String => Boolean): Boolean = {
    val func = "validateModelForPermissionCheck"
    // we need to have a device model to figure out whether the requesting entity has the permission to register a device/cew
    // we cannot fallback to DEVICE_ANY_MODIFY scope if the model is missing as it would be a security leak (MM: May 18, 2018)
    val result =
    for {
      model <- device.model.map(_.trim)
    } yield {
      body(model)
    }

    if (result.isEmpty) {
      logger.error("Device model missing")(
        DeviceConstants.FuncString -> func,
        DeviceConstants.PartnerIdString -> device.partnerId,
        DeviceConstants.DeviceIdString -> device.id,
        DeviceConstants.SerialString -> device.serial,
        DeviceConstants.ModelString -> device.model
      )
    }

    result.getOrElse(false)
  }

  private def validatePermissionToListDevices(agencyId: UUID, authenticatedRequest: JWTAuthenticatedRequest[AnyContent]): Either[Result, Unit] = {
    if (hasDeviceListPermissionAgency(agencyId, authenticatedRequest)) {
      Right(())
    } else {
      logger.warn("validatePermissionToListDevices")(
        "hasPermission" -> "false",
        "agencyId" -> agencyId,
        "subjectId" -> authenticatedRequest.jwt.subjectId,
        "subjectType" -> authenticatedRequest.jwt.subjectType
      )
      Left(errorResponse(resultStatus = Results.Forbidden, detail = Some("Requesting entity does not have permission to list devices")))
    }
  }


  private def validatePermissionToModifyDevice(agencyId: UUID, authenticatedRequest: JWTAuthenticatedRequest[AnyContent], device: Option[DeviceMetaData]): Either[Result, Unit] = {
    device.map { metadata =>
      if (hasModelBasedDeviceModifyPermission(agencyId, metadata, authenticatedRequest)) {
        Right(())
      } else {
        logger.warn("validatePermissionToModifyDevice")("hasPermission" -> "false", "agencyId" -> agencyId, "deviceId" -> metadata.id, "subjectId" -> authenticatedRequest.jwt.subjectId, "deviceOwner" -> device.map(_.owner))
        Left(errorResponse(Results.Forbidden))
      }
    }.getOrElse(Left(errorResponse(Results.Forbidden)))
  }

  private def validatePermissionToModifyDeviceAgency(
    agencyId: UUID,
    authenticatedRequest: JWTAuthenticatedRequest[AnyContent],
    device: Option[DeviceMetaData]
  ): Either[Result, Unit] = {
    device.map { metadata =>
      val hasPermission = validateModelForPermissionCheck(metadata) { model =>
        hasModelBasedDeviceModifyPermissionAgency(agencyId, model, authenticatedRequest)
      }
      if (hasPermission) {
        Right(())
      } else {
        logger.warn("validatePermissionToModifyDeviceAgency")("hasPermission" -> hasPermission, "agencyId" -> agencyId, "deviceId" -> metadata.id, "subjectId" -> authenticatedRequest.jwt.subjectId, "deviceOwner" -> device.map(_.owner))
        Left(errorResponse(Results.Forbidden))
      }
    }.getOrElse(Left(errorResponse(Results.Forbidden)))
  }

  /**
    * This method does the following,
    * 1] Given a list of device ID's, fetch device metadata from arsenal service (This is a single thrift call)
    * 2] Checks permissions for each device object and creates a mapping between device and the corresponding http status
    *
    * @return List of device metadata along with the http status
    */
  private def validatePermissionToModifyDeviceMetadata(
    agencyId: UUID,
    authenticatedRequest: JWTAuthenticatedRequest[JsValue],
    deviceIds: Seq[String],
    validatePermission: Option[DeviceMetaData] => Either[Result, Unit],
  ): Future[Either[Result, Seq[DeviceWithHttpStatusMetadata]]] = {
    def getDeviceIdMapping(deviceMetadataList: Seq[DeviceMetaData]): Map[String, DeviceMetaData] = {
      deviceMetadataList.flatMap { deviceMetadata =>
        for {
          deviceId <- deviceMetadata.id
        } yield {
          deviceId.toLowerCase -> deviceMetadata
        }
      }.toMap
    }

    def getDevicesWithHttpStatus(listDevicesResponse: ListDeviceResponse): Either[Result, Seq[DeviceWithHttpStatusMetadata]] = {
      val deviceMetadataList =
        listDevicesResponse.devices
          .filter(_.nonEmpty)
          .toRight {
            errorResponse(resultStatus = Results.InternalServerError, detail = Some("Device metadata missing in response when fetching devices"))
          }

      val result =
        deviceMetadataList match {
          case Left(error) => Left(error)
          case Right(metadataList) => {
            val deviceIdToMetadataMap = getDeviceIdMapping(metadataList)

            val devices: Seq[DeviceWithHttpStatusMetadata] =
              deviceIds.map { id =>
                val deviceMetadata = deviceIdToMetadataMap.get(id.toLowerCase)
                DeviceWithHttpStatusMetadata(id = id, device = deviceMetadata, httpStatusMetadata = HttpStatusMetadata(resultStatus = Results.Ok, httpStatus = OK, canonicalString = OkCanonicalString))
              }

            Right(devices)
          }
        }

      result
    }

    def checkPermission(devices: Seq[DeviceWithHttpStatusMetadata]): Either[Result, Seq[DeviceWithHttpStatusMetadata]] = {
      Option(devices).filter(_.nonEmpty)
        .toRight(Results.InternalServerError)
        .right.map { devicesWithHttpStatusMetadata =>
        val rv: Seq[DeviceWithHttpStatusMetadata] =
          devicesWithHttpStatusMetadata.map { dWithHttpStatusMetadata =>
            validatePermission(dWithHttpStatusMetadata.device) match {
              case Left(_) => dWithHttpStatusMetadata.copy(httpStatusMetadata = HttpStatusMetadata(resultStatus = Results.Forbidden, httpStatus = FORBIDDEN, title = Some(OperationNotPermittedString), detail = Some(OperationNotPermittedString), canonicalString = OperationNotPermittedCanonicalString))
              case Right(_) => dWithHttpStatusMetadata
            }
          }

        rv
      }
    }

    val devicesWithHttpStatusMetadata: FutureEither[Result, Seq[DeviceWithHttpStatusMetadata]] =
      for {
        listDevicesResponse <- FutureEither(listDevices(agencyId, deviceIds))
        devicesWithHttpStatus <- FutureEither.successful(getDevicesWithHttpStatus(listDevicesResponse))
        devices <- FutureEither.successful(checkPermission(devicesWithHttpStatus))
      } yield {
        devices
      }

    devicesWithHttpStatusMetadata.fold(result => Left(result), devices => Right(devices))
  }

  private def validateFriendlyName(friendlyName: Option[String]): Either[Result, String] = {
    def isValidFriendlyName(): Boolean = {
      friendlyName.exists(name => name.trim.length >= DeviceConstants.FriendlyNameMinLength
        && name.trim.length <= DeviceConstants.FriendlyNameMaxLength)
    }
    if(isValidFriendlyName()) {
      Right(friendlyName.get)
    } else {
      Left(errorResponse(Results.BadRequest))
    }
  }


  private def validatePermissionToRegisterDevice(agencyId: UUID, authenticatedRequest: JWTAuthenticatedRequest[JsValue], device: Option[DeviceMetaData]): Either[Result, Unit] = {
    device.map { metadata =>
      if (hasDeviceRegisterPermission(agencyId, metadata, authenticatedRequest.asInstanceOf[JWTAuthenticatedRequest[AnyContent]])) {
        Right(())
      } else {
        Left(errorResponse(Results.Forbidden))
      }
    }.getOrElse(Left(errorResponse(Results.Forbidden)))
  }

  private def validateUserExistsIfOwner(agencyId: UUID, ownerOpt: Option[EntityDescriptor]): Future[Either[Result, Unit]] = {
    ownerOpt
      .filter(owner => owner.entityType == TidEntities.Subscriber)
      .map(owner => {
      for {
        getUserResult <- komrade.getUser(agencyId.toString, owner.id)
        result <- Future.successful(
          getUserResult match {
            case Left(errorCode) =>
              if(errorCode == KomradeServiceErrorCode.NotFound)
                Left(arsenalErrorToResponse(ServiceErrorCode.OwnerInvalid))
              else
                Left(KomradeClient.errorToResponse(errorCode))
            case Right(_) => Right(())
          }
        )
      } yield result
    }).getOrElse(Future.successful(Right(())))
  }



  private def validateUpdateRequest(params: DeviceUpdateRequest): Either[Result, DeviceUpdateRequest] = {
    val invalid =
      StringUtils.isNullOrWhiteSpace(params.id) ||
      (!StringUtils.isNullOrWhiteSpace(params.ownerType) && StringUtils.isNullOrWhiteSpace(params.ownerId)) ||
      (StringUtils.isNullOrWhiteSpace(params.ownerType) && !StringUtils.isNullOrWhiteSpace(params.ownerId)) ||
        // If ownerId is present, ensure that it is a proper UUID
      (params.ownerId.isDefined && Convert.tryToUuid(params.ownerId.get).isEmpty) ||
      params.model.contains(AxonSnfServer.toString)

    if (invalid) {
      Left(errorResponse(Results.BadRequest))
    } else if (params.friendlyName.isDefined && params.friendlyName.exists(s => s.contains('\r') || s.contains('\n'))) {
      Left(errorResponse(Results.BadRequest, Some("Invalid friendly name")))
    } else {
      Right(params)
    }
  }

  private def validateNumberRange(numberInput: Int, minInclusive: Int, maxInclusive: Int) : Either[Result, Int] = {
    Option(numberInput).filter(num => (num >= minInclusive && num <= maxInclusive))
      .toRight(errorResponse(Results.BadRequest, detail = Some( s"Number '$numberInput' not within range")))
  }

  private def groupIdToTeamId(id: UUID, partnerId: String): Option[TeamId] = {
    for {
      parsedDomain <- Convert.tryToUuid(partnerId)
    } yield {
      TeamId(id = id, domain = parsedDomain)}
  }

  def transformUrl(func: String, partnerId: String, deviceIdOpt: Option[String], versionOpt: Option[String], firmwareDownloadResponse: GetFirmwareDownloadResponse, downloadFileName: Option[String]): FutureEither[Result, String] = {
    statsd.incrementCounter("build_firmware_download_url")
    logger.info("Building download url")(
      DeviceConstants.FuncString -> func,
      DeviceConstants.AgencyIdString -> partnerId,
      DeviceConstants.DeviceIdString -> deviceIdOpt.getOrElse("n/a"),
      DeviceConstants.FirmwareVersionString -> versionOpt.getOrElse("latest"))

    val serverError = errorResponse(Results.InternalServerError)
    val notFoundError = errorResponse(Results.NotFound)

    for {
      firmware <- FutureEither.successful(firmwareDownloadResponse.firmware.toRight(notFoundError))
      downloadUrl <- FutureEither.successful(firmware.downloadUrl.toRight(notFoundError))
      partner <- FutureEither(komrade.getPartnerFromId(partnerId)).mapLeft(_ => serverError)
      domain <- FutureEither.successful(partner.domain.toRight(serverError))
      transformedUrl <- FutureEither.successful(transform(downloadUrl, domain, downloadFileName).toRight(serverError))
    } yield {
      statsd.incrementCounter("build_firmware_download_url_success")
      logger.info("Returning download url redirect")(
        DeviceConstants.FuncString -> func,
        DeviceConstants.AgencyIdString -> partnerId,
        DeviceConstants.DeviceIdString -> deviceIdOpt.getOrElse("n/a"),
        DeviceConstants.ValueString -> transformedUrl)
      transformedUrl
    }
  }

  case class DeviceUpdateRequest(id: Option[String], serial: Option[String], model: Option[String], make: Option[String], friendlyName: Option[String],
    ownerId: Option[String], ownerType: Option[String], preferences: Option[Seq[Preference]] = None, dateLastUpload: Option[String] = None)

  private case class AdmModelMetadata(displayName: String, alias: Seq[String], serialPrefix: Seq[String])

  private case class DeviceStatusUpdateRequest(deviceIds: Seq[String], newStatus: DeviceStatus2)

  case class EdcaBulkDeviceTransferRequest(currentAgency: Option[String], targetAgency: Option[String], model: Option[String], deviceIds: Seq[String])

  case class EdcaSendAdminCommandsRequest(command: Option[String], serial: Option[String], model: Option[String], partnerId: Option[String],
                                          expirationEpochSeconds: Option[Long], idempotencyKey: Option[String], action: Option[String])
}

