#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import math


if __name__ == "__main__":
    spark = SparkSession.builder.appName('Spark_3_1').master('yarn').getOrCreate()

    schema = StructType(fields=[
        StructField("stop", StringType())
    ])

    dataset_stop = '/data/wiki/stop_words_en-xpo6.txt'
    df_stop = spark.read.format("csv").schema(schema).option("sep", "\n").load(dataset_stop)

    df_stop_collect = df_stop.collect()
    stop_word_list = [row.stop for row in df_stop_collect]


    schema_data = StructType(fields=[
        StructField("id", IntegerType()),
        StructField("txt", StringType())
    ])

    dataset = '/data/wiki/en_articles_part'

    df = spark.read.format("csv").schema(schema_data).option("sep", "\t").load(dataset).cache()

    df = df.filter(col("txt") != "")

    word = (
        df.withColumn("word", explode(split(col("txt"), "\s+")))
        .withColumn("word", lower(col('word')))
        .withColumn("word", regexp_replace("word", "^\W+|\W+$", ""))
    ).select(col('word')).coalesce(1).cache()



    word = word.filter(~word.word.isin(stop_word_list))#.show()
    word = word.filter(col("word") != "")  # .show()


    pair = word.alias('w1').select(col('word').alias('word1'), col('word').alias('word2'), lit("A").alias('A'))

    pair = pair.withColumn('original_order', monotonically_increasing_id())
    pair = pair.withColumn('lead', lead('word2').over(Window.orderBy('original_order')))

    pair = pair.drop('A', 'original_order')

    req_column = ['word1',lit('_').alias('my'),'lead']
    pair = pair.withColumn('concatenated_cols',concat(*req_column))

    pair.coalesce(1).cache()


    pair_list = (
        pair.select(col('concatenated_cols'), col('word1'), col('lead'))
        .groupBy("concatenated_cols", 'word1', 'lead')
        .count()
        .sort("count", ascending=False)
    ).coalesce(1).cache()


    count_all_pairs = pair_list.agg(sum("count").alias("count_all_words")).coalesce(1).cache()


    pair_list = pair_list.select('concatenated_cols', 'word1', 'lead', 'count').where('count >= 500')


    word_counts = (
        word.select(col('word'))
        .groupBy("word")
        .count()
        .sort("count", ascending=False)
    )

    word_count_list = word_counts.coalesce(1).cache()



    word_count_total_df = word_counts.agg(sum("count").alias("count_all_words")).coalesce(1).cache()
    word_count_total = word_count_total_df.cache() # number of all words

    pair_count_total = count_all_pairs # number of pairs


    word_count_total_number = word_count_total.collect()
    wc = [row.count_all_words for row in word_count_total_number]
    x = wc[0]


    pair_count_total_number = pair_count_total.collect()
    wc = [row.count_all_words for row in pair_count_total_number]
    y = wc[0]


    p_word = word_count_list.select('word', 'count', (col('count')/x).alias('P(word)'))#.show(5)


    p_pair = pair_list.select('concatenated_cols', 'count', (col('count')/y).alias('P(pair)'))#.show(5)


    t1 = (
        pair_list.alias('p').join(
        p_word.alias('pa'),
        col('p.word1') == col('pa.word')
    ).select(col('p.concatenated_cols'), col('word1'), col('lead'), col('pa.P(word)').alias('p_word1')))


    t2 = (
        t1.alias('p').join(p_word.alias('pa'),
        col('p.lead') == col('pa.word')
        )
    .select(col('p.concatenated_cols'), col('word1'), col('lead'),
            col('p.p_word1'),
            col('pa.P(word)').alias('p_lead')
           )
    )


    t3 = (
        t2.alias('p').join(p_pair.alias('pp'),
                           col('p.concatenated_cols') == col('pp.concatenated_cols')
                           )
        .select(
            col('p.concatenated_cols'),
            col('p.word1'),
            col('p.lead'),
            col('p.p_word1'),
            col('p.p_lead'),
            col('pp.P(pair)').alias('p_pair')
        )
    )

    t3 = t3.drop('word1', 'lead').coalesce(1).cache()



    t3 = t3.withColumn('Pa_x_Pb', col('p_word1') * col('p_lead'))
    t3 = t3.withColumn('R_', col('p_pair') / col('Pa_x_Pb'))
    t3 = t3.withColumn('R', log(math.e, col('R_')))
    t3 = t3.withColumn('for_pmi', log(math.e, col('p_pair')))
    pmi = t3.select('concatenated_cols', 'R', 'for_pmi')#.show(5)


    pmi = pmi.withColumn('result', col('R') / col('for_pmi'))

    npmi = pmi.withColumn('result-', col('result') * (-1))

    ########################

    r = npmi.select('concatenated_cols', 'result-').orderBy(desc('result-'))

    p = r.select('concatenated_cols').take(39)
    out = [row.concatenated_cols for row in p]

    print('\n'.join(out))
