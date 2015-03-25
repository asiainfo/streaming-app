package com.asiainfo.ocdc.streaming.rule

import com.asiainfo.ocdc.streaming.source.SourceObj


class ExampleRule extends LabelRule {

  def attachLabel(item: SourceObj): SourceObj = {
    item
  }
}
