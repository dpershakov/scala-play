package utils.data

import play.api.libs.json.Json

case class DeviceOwnerMapping(
deviceIds: Seq[String],
owner: String) {
}

object DeviceOwnerMapping {
  implicit def jsonFormat = Json.format[DeviceOwnerMapping]
}
