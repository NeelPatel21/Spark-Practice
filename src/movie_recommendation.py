from pyspark import SparkConf, SparkContext
import util as u

def id_mov_format(row):
    y = row.split(',', 2)
    return int(y[0]), y[1]


def topRecommendation(rdd, mid, br, n=10, rating=50):
    # keys = br.keys()
    rddt = rdd.filter(lambda x: (x[0] == mid or x[1] == mid) and x[3] > rating)
    rddt = rddt.map(lambda x: ((x[0] if x[1] == mid else x[1]), x[2], x[3])).sortBy(lambda x: -x[1])
    return rddt.take(n)


def rec_format(row):
    ar = row.split(',')
    return int(ar[0]), int(ar[1]), float(ar[2]), int(ar[3])


if __name__ == '__main__':
    # conf = SparkConf().setAppName('movie-recommendation').setMaster('local')#.set('spark.local.dir', 'C:/temp')
    sc = SparkContext()

    rdd2 = sc.textFile('D:/Data/ML Databases/movie_rec/recommendation_0.0-10.csv')
    rdd2 = rdd2.map(rec_format)#.collect()

    key = str(input('Enter a keyword to search movie..'))

    rdd1 = sc.textFile('D:/Data/ML Databases/movie_rec/id_movie.csv')
    id_mov = rdd1.map(id_mov_format).collectAsMap()

    print('Movie_ID\tMovie_Title\n')
    for k, v in id_mov.items():
        if key.lower() in v.lower():
            print('{0}\t{1}\n'.format(k, v))

    mid = int(input('Enter Id of the movie..'))
    # u.printf(rdd2.collect())

    print('Movie_ID\tMovie_Title\tConfidence\tCommon_Ratings\n')
    # u.printf(topRecommendation(rdd2, 105, id_mov))
    for i in topRecommendation(rdd2, mid, id_mov, 10, 100):
        if i[0] in id_mov.keys():
            print('{0}\t{1}\t{2}\t{3}\n'.format(str(i[0]), str(id_mov[i[0]]), str(i[1]), str(i[2])))
        else:
            print('bug')
