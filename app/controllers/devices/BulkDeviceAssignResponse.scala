package controllers.devices

import com.evidence.service.arsenal.thrift.BulkAssignDeviceResponse
import play.api.libs.json.Writes
import services.arsenal.ArsenalConversions
import utils.JsonApiOrg

object BulkDeviceAssignResponse extends JsonApiOrg with ArsenalConversions {

  implicit val createBulkAssignResponseWriter: Writes[BulkAssignDeviceResponse] = { resp =>
    buildBulkAssignErrors(resp.errors.toMap)
  }
}
