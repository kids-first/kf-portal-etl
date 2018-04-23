package io.kf.etl.processors.download

import io.kf.etl.external.dataservice.entity.EGenomicFile

object ScalaPBJson4sMain extends App {
  
  val jsonstr =
    """
      {
            "controlled_access": true, 
            "created_at": "2018-04-18T20:22:31.402367+00:00", 
            "data_type": "submitted aligned reads index", 
            "file_format": "crai", 
            "file_name": "06-0015.cram.crai", 
            "hashes": {
              "md5": "c0f34d2e1894b63e93d16374fde8dc3d"
            }, 
            "is_harmonized": false, 
            "kf_id": "GF_Q88Y2HC8", 
            "latest_did": "6fe283fc-0a89-4313-bb65-9e2fa3e1eef5", 
            "metadata": {}, 
            "modified_at": "2018-04-18T20:22:31.402392+00:00", 
            "reference_genome": null, 
            "size": 1332670, 
            "urls": [
              "s3://kf-seq-data-broad/fc-ff4e8f53-e153-4c78-b630-0ebe66030d80/GMKF_Chung_CDH_WGS_V1/RP-1370/WGS/06-0015/v2/06-0015.cram.crai"
            ]
          }
    """.stripMargin

  val parser = new com.trueaccord.scalapb.json.Parser(preservingProtoFieldNames = true)

  val json = parser.fromJsonString[EGenomicFile](jsonstr)

  println(json)

}
