# encoding=gb18030
import multiprocessing
import sys

import constant
import utils
from constant import root_path

logger = utils.get_logger('sparkTask')
input = "/user/go2data_rank/taoyongbo/input/"
output = "/user/go2data_rank/taoyongbo/output/"


def poi_task(environment='beta'):
    logger.info("spark poi_task process,environment:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)

    commond = "spark-submit --master yarn --deploy-mode cluster --name PoiTask --class cluster.task.PoiTask --executor-memory 4G --num-executors 19 --executor-cores 5  --conf spark.default.parallelism=300  " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark poi_task finished")


def structure_task(environment='beta'):
    logger.info("spark structure_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name StructureTask --class cluster.task.StructureTask --executor-memory 4G --num-executors 19 --executor-cores 5 --conf spark.default.parallelism=350 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark structure_task finished")


def matchcount_task(environment='beta'):
    logger.info("spark matchcount_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name MatchCountTask --class cluster.task.MatchCountTask --executor-memory 4G --num-executors 19 --executor-cores 5  --conf spark.default.parallelism=3000 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark matchcount_task finished")


def rank_optimize_task(environment='beta'):
    logger.info("spark rank_optimization process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name RankOptimizeTask --class cluster.task.RankOptimizeTask  --jars " + libjars + " --executor-memory 4G --num-executors 19 --executor-cores 5 --conf spark.default.parallelism=350 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark rank_optimization finished")


def rank_distcp():
    logger.info("rank_distcp process")

    commond0 = "hadoop fs -rmr " + constant.yarn_multiRank_output_path + " " + constant.yarn_hotCountRank_output_path + " " + constant.yarn_hitCountRank_output_path
    utils.execute_command(commond0, shell=True)

    commond1 = "hadoop distcp -overwrite -m 50 " + constant.zeus_path + constant.zeus_multiRank_path + " " + constant.yarn_multiRank_output_path
    utils.execute_command(commond1, shell=True)

    commond2 = "hadoop distcp -overwrite -m 50 " + constant.zeus_path + constant.zeus_hotCountRank_path + " " + constant.yarn_hotCountRank_output_path
    utils.execute_command(commond2, shell=True)

    commond3 = "hadoop distcp -overwrite -m 50 " + constant.zeus_path + constant.zeus_hitCountRank_path + " " + constant.yarn_hitCountRank_output_path
    utils.execute_command(commond3, shell=True)

    logger.info("rank_distcp finished")


def poiXml_myself_distcp(zeus_myself_path=None):
    logger.info("poiXml_myself_distcp process")

    if zeus_myself_path is not None:
        constant.zeus_myself_path = zeus_myself_path

    rm_commond3 = "hadoop fs -rmr " + constant.yarn_myself_input_path
    utils.execute_command(rm_commond3, shell=True)

    commond3 = "hadoop distcp -overwrite -m 30 " + constant.zeus_path + constant.zeus_myself_path + " " + constant.yarn_myself_input_path
    utils.execute_command(commond3, shell=True)
    logger.info("poiXml_myself_distcp finished")


def poiXml_poi_distcp(zeus_poi_path=None):
    logger.info("poiXml_poi_distcp process")

    if zeus_poi_path is not None:
        constant.zeus_poi_path = zeus_poi_path

    rm_commond1 = "hadoop fs -rmr " + constant.yarn_poi_input_path
    utils.execute_command(rm_commond1, shell=True)

    commond1 = "hadoop distcp -overwrite " + constant.zeus_path + constant.zeus_poi_path + " " + constant.yarn_poi_input_path
    utils.execute_command(commond1, shell=True)

    logger.info("poiXml_poi_distcp finished")


def poiXml_buspoi_distcp(zeus_buspoi_path=None):
    logger.info("poiXml_buspoi_distcp process")

    if zeus_buspoi_path is not None:
        constant.zeus_buspoi_path = zeus_buspoi_path

    rm_commond2 = "hadoop fs -rmr " + constant.yarn_buspoi_input_path
    utils.execute_command(rm_commond2, shell=True)

    commond2 = "hadoop distcp -overwrite " + constant.zeus_path + constant.zeus_buspoi_path + " " + constant.yarn_buspoi_input_path
    utils.execute_command(commond2, shell=True)

    logger.info("poiXml_buspoi_distcp finished")


def xml_distcp(zeus_poi_path, zeus_buspoi_path, zeus_myself_path, zeus_structure_path, zeus_polygon_path):
    pool = multiprocessing.Pool(processes=5)
    for i in range(1, 6):
        if i == 1:
            pool.apply_async(poiXml_poi_distcp, (zeus_poi_path,))
        if i == 2:
            pool.apply_async(poiXml_myself_distcp, (zeus_myself_path,))
        if i == 3:
            pool.apply_async(poiXml_buspoi_distcp, (zeus_buspoi_path,))
        if i == 4:
            pool.apply_async(structure_distcp, (zeus_structure_path,))
        if i == 5:
            pool.apply_async(polygon_distcp, (zeus_polygon_path,))

    pool.close()
    pool.join()


def structure_distcp(zeus_structure_path=None):
    if zeus_structure_path is not None:
        constant.zeus_structure_path = zeus_structure_path
    logger.info("structure_distcp process")

    rm_commond = "hadoop fs -rmr " + constant.yarn_structure_input_path
    utils.execute_command(rm_commond, shell=True)

    commond = "hadoop distcp -overwrite " + constant.zeus_path + constant.zeus_structure_path + " " + constant.yarn_structure_input_path
    utils.execute_command(commond, shell=True)
    logger.info("structure_distcp finished")


def polygon_distcp(zeus_polygon_path=None):
    if zeus_polygon_path is not None:
        constant.zeus_polygon_path = zeus_polygon_path

    logger.info("polygon_distcp process")

    rm_commond = "hadoop fs -rmr " + constant.yarn_polygon_input_path
    utils.execute_command(rm_commond, shell=True)

    commond = "hadoop distcp -overwrite " + constant.zeus_path + constant.zeus_polygon_path + " " + constant.yarn_polygon_input_path
    utils.execute_command(commond, shell=True)
    logger.info("polygon_distcp finished")


def gps_distcp():
    logger.info("gps_distcp process")
    rm_commond = "hadoop fs -rmr " + constant.yarn_gps_input_path
    utils.execute_command(rm_commond, shell=True)

    commond = "hadoop distcp -overwrite  -m 50 " + constant.zeus_path + constant.zeus_gps_path + " " + constant.yarn_gps_input_path
    utils.execute_command(commond, shell=True)
    logger.info("gps_distcp finished")

def matchCount_distcp():
    logger.info("matchCount_distcp process")
    rm_commond = "hadoop fs -rmr " + constant.yarn_matchCount_input_path
    utils.execute_command(rm_commond, shell=True)

    commond = "hadoop distcp   -update -skipcrccheck -m 50 " +constant.yarn_matchCount_output_path + " " + constant.yarn_matchCount_input_path
    utils.execute_command(commond, shell=True)
    logger.info("matchCount_distcp finished")

def rankCombine_task(environment='beta'):
    logger.info("spark rankCombine_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name RankCombineTask --class cluster.task.RankCombineTask --jars " + libjars + " --executor-memory 4G --num-executors 19 --executor-cores 5 --conf spark.default.parallelism=350 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output

    utils.execute_command(commond, shell=True)
    logger.info("spark rankCombine_task finished")


def featureCombine_task(environment='beta'):
    logger.info("spark featureCombine_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name FeatureCombineTask --class cluster.task.FeatureCombineTask --jars " + libjars + " --executor-memory 4G --num-executors 26 --executor-cores 6 --driver-memory 6G --conf spark.default.parallelism=350 --conf spark.storage.memoryFraction=0.5 --conf spark.shuffle.memoryFraction=0.3 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark featureCombine_task finished")


def featureConvert_task(environment='beta'):
    logger.info("spark featureConvert_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name FeatureConvertTask --class cluster.task.FeatureConvertTask --jars " + libjars + " --executor-memory 4G --num-executors 19 --executor-cores 5 --conf spark.default.parallelism=350 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark featureConvert_task finished")


def brandRank_task(environment='beta'):
    logger.info("spark brandRank_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name BrandRankTask --class cluster.task.BrandRankTask --jars " + libjars + " --executor-memory 4G --num-executors 19 --executor-cores 5 --conf spark.default.parallelism=350 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark brandRank_task finished")


def gpsPopularity_task(environment='beta'):
    logger.info("spark gpsPopularity_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name GpsPopularityTask --class cluster.task.GpsPopularityTask --jars " + libjars + " --executor-memory 4G --num-executors 19 --executor-cores 5 --conf spark.default.parallelism=3000 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark gpsPopularity_task finished")


def hotCountRank_task(environment='beta'):
    logger.info("spark hotCountRank_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name HotCountRankTask --class cluster.task.HotCountRankTask --jars " + libjars + " --executor-memory 11G --num-executors 10 --executor-cores 5 --conf spark.default.parallelism=1 --conf spark.storage.memoryFraction=0.2 --conf spark.shuffle.memoryFraction=0.6 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark hotCountRank_task finished")


def hitCountRank_task(environment='beta'):
    logger.info("spark hitCountRank_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name HitCountRankTask --class cluster.task.HitCountRankTask --jars " + libjars + " --executor-memory 11G --num-executors 10 --executor-cores 5 --conf spark.default.parallelism=1 --conf spark.storage.memoryFraction=0.2 --conf spark.shuffle.memoryFraction=0.6 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark hitCountRank_task finished")


def poirank_task(environment='beta'):
    logger.info("spark poirank_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name PoiRankTask --class cluster.task.PoiRankTask --jars " + libjars + " --executor-memory 10G --num-executors 20 --executor-cores 5 --driver-memory 10G --driver-cores 10 --conf spark.default.parallelism=1  " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark poirank_task finished")


def polygonRank_task(environment='beta'):
    logger.info("spark polygonRank_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name PolygonRankTask --class cluster.task.PolygonRankTask --jars " + libjars + " --executor-memory 11520M --num-executors 20 --executor-cores 15  --driver-memory 25G --driver-cores 10 --conf spark.default.parallelism=1 --conf spark.storage.memoryFraction=0.4 --conf spark.shuffle.memoryFraction=0.6 --conf spark.shuffle.consolidateFiles=true " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark polygonRank_task finished")

def structureRankCompare_task(environment='beta'):
    logger.info("spark structureRankCompare_task process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name StructureRankCompareTask --class testTasks.StructureRankCompareTask --jars " + libjars + " --executor-memory 4G --num-executors 19 --executor-cores 5 --conf spark.default.parallelism=350 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark structureRankCompare_task finished")


def show_menu(task_menu):
    print("***************current task list*****************")

    sort_task_menu = sorted(task_menu.items(), key=lambda kv: kv[0])

    for k, item in sort_task_menu:
        print("{key}: {name} ".format(key=k, name=item['name']))

    print("***************current task list*****************")
    print("please input taskId for your choice")


def numbers_to_functions_to_strings(argument):
    """
    Execute the function
    """
    funcInfo = task_menu.get(argument, lambda: "nothing")
    func = funcInfo.get('function')
    return func()


task_menu = {
    1: dict(name="xml_distcp", function=poiXml_myself_distcp),
    2: dict(name="structure_distcp", function=structure_distcp),
    3: dict(name="polygon_distcp", function=polygon_distcp),
    4: dict(name="gps_distcp", function=gps_distcp),
    5: dict(name="poi_task", function=poi_task),
    6: dict(name="matchcount_task", function=matchcount_task),
    7: dict(name="structure_task", function=structure_task),
    8: dict(name="featureCombine_task", function=featureCombine_task),
    9: dict(name="featureConvert_task", function=featureConvert_task),
    10: dict(name="gpsPopularity_task", function=gpsPopularity_task),
    11: dict(name="poirank_task", function=poirank_task),
    12: dict(name="hotCountRank_task", function=hotCountRank_task),
    13: dict(name="hitCountRank_task", function=hitCountRank_task),
    14: dict(name="rankCombine_task", function=rankCombine_task),
    15: dict(name="polygonRank_task", function=polygonRank_task),
    16: dict(name="brandRank_task", function=brandRank_task),
    17: dict(name="rank_optimize_task", function=rank_optimize_task),
    18: dict(name="matchCount_distcp", function=matchCount_distcp),
    19:dict(name="structureRankCompare_task", function=structureRankCompare_task),

}


def main():
    show_menu(task_menu)

    number = sys.stdin.readline().strip()
    if not number.isdigit() or int(number) not in task_menu.keys():
        logger.warning('taskId is invalid!')
        sys.exit()

    numbers_to_functions_to_strings(int(number))
    sys.exit()


if __name__ == '__main__':
    main()
