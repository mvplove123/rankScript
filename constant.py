# encoding=gb18030
lib_path = "/search/odin/taoyongbo/rank/prod/scala_spark/lib"
jar_path = "/search/odin/taoyongbo/rank/prod/scala_spark/"
java_jar_path = "/search/odin/taoyongbo/rank/java_spark/"
local_featurePoi_path = '/search/odin/taoyongbo/rank/featurePoi/'
local_city_featurePoi_path = '/search/odin/taoyongbo/rank/cityFeaturePoi/'
rank_path = "/search/odin/taoyongbo/rank/rankResult/"

root_path = '/search/odin/taoyongbo/rank/'



zeus_path = "hftp://master01.zeus.hadoop.sogou:50070"
yarn_path = "hdfs://master01.yarn.hadoop.sogou:6230"

# poi xml original files
zeus_poi_path = "/user/go2data/sdb_data/all_data/nochange_data/2016-10-20/result/POI"
yarn_poi_input_path = "/user/go2data_rank/taoyongbo/input/poiXml1"

zeus_buspoi_path = "/user/go2data/sdb_data/all_data/nochange_data/2016-10-20/result/BUSPOI"
yarn_buspoi_input_path = "/user/go2data_rank/taoyongbo/input/poiXml2"

zeus_myself_path = "/user/go2data/sdb_data/all_data/poi_data/2016-10-31/raw_data/myself"
yarn_myself_input_path = "/user/go2data_rank/taoyongbo/input/poiXml3"

# name structure original files
zeus_structure_path = "/user/go2data/huajin.shen_dev/structure_by_name/2016-10-20/name_prefix_structure_release"
yarn_structure_input_path = "/user/go2data_rank/taoyongbo/input/nameStructure"

# matchCount
zeus_matchCount_path = "/user/go2search/taoyongbo/output/caculate"
yarn_matchCount_input_path = "/user/go2data_rank/taoyongbo/input/matchCount"
yarn_matchCount_output_path = "/user/go2data_rank/taoyongbo/output/matchCount"

# gpsHot
zeus_gps_path = "/user/go2search/taoyongbo/output/gps"
yarn_gps_input_path = "/user/go2data_rank/taoyongbo/input/gps"

# polygon
zeus_polygon_path = "/user/go2data/sdb_data/all_data/nochange_data/2016-10-20/result/POLYGON/"
yarn_polygon_input_path = "/user/go2data_rank/taoyongbo/input/polygonXml"

# poiHotCount
yarn_poiHotCount_input_path = "/user/go2data_rank/taoyongbo/input/poiHotCount"

# searchCount
yarn_searchCount_input_path = "/user/go2data_rank/taoyongbo/input/searchCount"

upload_local_path = '/search/odin/taoyongbo/rank/result/'

rsync_version_path = '/search/odin/taoyongbo/rank/rsync_version/'


#back_rank
back_rank_path = '/search/odin/taoyongbo/rank/back_rank/'



# poi rank
zeus_multiRank_path = "/user/go2search/taoyongbo/input/multiRank/"
zeus_hotCountRank_path = "/user/go2search/taoyongbo/input/hotCountRank/"
zeus_hitCountRank_path = "/user/go2search/taoyongbo/input/hitCountRank/"

yarn_multiRank_output_path = "/user/go2data_rank/taoyongbo/output/multiRank/"
yarn_hotCountRank_output_path = "/user/go2data_rank/taoyongbo/output/hotCountRank/"
yarn_hitCountRank_output_path = "/user/go2data_rank/taoyongbo/output/hitCountRank/"


