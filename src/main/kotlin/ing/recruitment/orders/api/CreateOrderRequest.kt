package ing.recruitment.orders.api

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size
import java.io.IOException

data class CreateOrderRequest(
    @field:NotBlank
    @field:Size(max = 255)
    @field:JsonDeserialize(using = StrictJsonStringDeserializer::class)
    val customerId: String,
)

class StrictJsonStringDeserializer : JsonDeserializer<String>() {
    @Throws(IOException::class)
    override fun deserialize(parser: JsonParser, context: DeserializationContext): String =
        if (parser.currentToken == JsonToken.VALUE_STRING) {
            parser.text
        } else {
            context.handleUnexpectedToken(String::class.java, parser) as String
        }
}
