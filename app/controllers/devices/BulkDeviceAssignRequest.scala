package controllers.devices

import com.evidence.api.thrift.v1.TidEntities
import com.evidence.service.arsenal.thrift.ServiceErrorCode
import play.api.libs.json.{JsError, JsSuccess, Json, JsonValidationError, Reads, Writes}
import services.ecom.{EcomJson, EcomThriftHelpers}
import utils.JsonApiOrg
import utils.data.DeviceOwnerMapping
import scala.collection.Map

case class BulkDeviceAssignRequest(deviceAssignments: List[DeviceOwnerMapping])
  extends JsonApiOrg with EcomJson with BulkAssignDevicesRequestJsonFields {

  lazy val validationError: Option[String] = {

    val deviceAssignmentSizeError = if (deviceAssignments.nonEmpty) None else Some("at least one device assignment must be specified")

    val errorList = List(deviceAssignmentSizeError).flatten
    if (errorList.isEmpty) None else Some(errorList.mkString("; "))
  }

  lazy val isValid: Boolean = validationError.isEmpty
}

object BulkDeviceAssignRequest extends JsonApiOrg with EcomThriftHelpers with EcomJson with BulkAssignDevicesRequestJsonFields {

  implicit val bulkDeviceAssignReads: Reads[BulkDeviceAssignRequest] = Reads { json =>

    val requestObj = for {
      assignmentArray <- (json \ deviceAssignmentField).validate[List[DeviceOwnerMapping]]
    } yield BulkDeviceAssignRequest(assignmentArray)

    requestObj match {
      case ok@JsSuccess(request, _) if request.isValid => ok
      case JsSuccess(request, path) => // successful parsing, invalid request
        JsError(path, JsonValidationError(request.validationError.getOrElse("invalid bulk assign request")))
      case alreadyFailed => alreadyFailed
    }
  }
}

private[devices] trait BulkAssignDevicesRequestJsonFields {
  protected val BulkAssignDeviceRequestJsonType = "case_bulk_assign_devices_request"
  protected val deviceAssignmentField = "deviceAssignment"
}