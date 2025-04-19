package com.insite.streams.common.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtils extends Serializable {
  @transient lazy val mapper: ObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)
}

