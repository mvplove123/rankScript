# encoding=gb18030
lib_path = "/search/odin/taoyongbo/rank/beta/scala_spark/lib"
jar_path = "/search/odin/taoyongbo/rank/beta/scala_spark/"
java_jar_path = "/search/odin/taoyongbo/rank/java_spark/"
local_featurePoi_path = '/featurePoi/'
local_brandfeaturePoi_path = '/brandFeaturePoi/'
local_city_featurePoi_path = '/cityFeaturePoi/'
local_multi_path = "/multi/"
brand_rank_path = "/brandRank/"

hotCount_single_rank_path = "/single/hotCount/"
hitCount_single_rank_path = "/single/hitCount/"

weight_path = '/config/poi-weight.txt '
poi_threshold_path = '/config/poi-threshold.txt'

local_structure_optimize_path = '/result/structureOptimizeRank'
local_structure_rank_path = '/result/structureRank'
local_split_featurePoi_path = '/splitfeaturePoi/'

rank_version_path = "/search/odin/taoyongbo/rank/rankVersion/"

root_path = '/search/odin/taoyongbo/rank/'

zeus_path = "hftp://master01.zeus.hadoop.sogou:50070"
yarn_path = "hdfs://master01.yarn.hadoop.sogou:6230"
yarn_mars_path = "hdfs://master01.mars.hadoop.sogou:6230"

# http://master01.zeus.hadoop.sogou
default_rank_output_path = "taoyongbo/rank/20171227/"

# poi xml original files
zeus_poi_path = "/user/go2data/sdb_data/all_data/nochange_data/2016-10-20/result/POI"
yarn_poi_input_path = "/poiXml1"

zeus_buspoi_path = "/user/go2data/sdb_data/all_data/nochange_data/2016-10-20/result/BUSPOI"
yarn_buspoi_input_path = "/poiXml2"

zeus_myself_path = "/user/go2data/sdb_data/all_data/poi_data/2016-10-31/raw_data/myself"
yarn_myself_input_path = "/poiXml3"

# name structure original files
zeus_structure_path = "/user/go2data/huajin.shen_dev/structure_by_name/2016-10-20/name_prefix_structure_release"
yarn_structure_input_path = "/nameStructure"

# matchCount
zeus_matchCount_path = "/user/go2search/taoyongbo/output/caculate"
yarn_matchCount_input_path = "/user/go2data_rank/taoyongbo/input/matchCount"
yarn_matchCount_output_path = "/user/go2data_rank/taoyongbo/output/matchCount"

# gpsHot
zeus_gps_path = "/user/go2search/taoyongbo/output/gps"
yarn_gps_input_path = "/gps"

# polygon
zeus_polygon_path = "/user/go2data/sdb_data/all_data/nochange_data/2016-10-20/result/POLYGON/"
yarn_polygon_input_path = "/polygonXml"

# poiHotCount
yarn_poiHotCount_input_path = "/user/go2data_rank/taoyongbo/input/poiHotCount"

# searchCount
yarn_searchCount_input_path = "/searchCount"

upload_local_path = '/search/odin/taoyongbo/rank/result/'

rsync_version_path = '/search/odin/taoyongbo/rank/rsync_version/'

# poi rank
zeus_multiRank_path = "/user/go2search/taoyongbo/input/multiRank/"
zeus_hotCountRank_path = "/user/go2search/taoyongbo/input/hotCountRank/"
zeus_hitCountRank_path = "/user/go2search/taoyongbo/input/hitCountRank/"

yarn_multiRank_output_path = "/user/go2data_rank/taoyongbo/output/multiRank/"
yarn_hotCountRank_output_path = "/user/go2data_rank/taoyongbo/output/hotCountRank/"
yarn_hitCountRank_output_path = "/user/go2data_rank/taoyongbo/output/hitCountRank/"

# filter rank source

# similarQueryCount
yarn_similarQueryCount_input_path = "/similarQueryCount/"

# sogouViewCount
zeus_sogouViewCount_path = "/user/go2search/taoyongbo/output/20170921sougouViewCount"
yarn_sogouViewCount_input_path = "/user/go2data_rank/taoyongbo/input/filterRank/sogouViewCount/"

# vrHitCount
zeus_vrHitCount_path = "/user/go2data_crawler/dc_log/VR_HITCOUNT"
yarn_vrHitCount_input_path = "/user/go2data_rank/taoyongbo/input/filterRank/vrHitCount/VR_HITCOUNT"

# vrViewCount
zeus_vrViewCount_path = "/user/go2data_crawler/dc_log/VR_VIEW"
yarn_vrViewCount_input_path = "/user/go2data_rank/taoyongbo/input/filterRank/vrViewCount/VR_VIEW"

# filterPoi
yarn_filterPoi_input_path = "/filterPoi/"

local_sign = "local"
cluster_sign = "hadoop"
