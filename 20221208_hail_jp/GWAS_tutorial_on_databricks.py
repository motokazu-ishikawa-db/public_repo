# Databricks notebook source
# MAGIC %md
# MAGIC # 「Databricksでhailの環境を立ち上げてみました」
# MAGIC ## 2022年12月8日 hail-jp 第6回ウェビナー 発表3
# MAGIC 本ノートブックは、[GWAS Tutorial]( https://hail.is/docs/0.2/tutorials/01-genome-wide-association-study.html )をDatabricksで再現したノートブックです

# COMMAND ----------

# MAGIC %md
# MAGIC ##　動作のための前提条件
# MAGIC - hail 0.2.85がセットアップされているクラスターにアタッチされていること
# MAGIC - Databricks Runtime versionが10.4 LTSであること 
# MAGIC - インターネットからのデータをダウンロードするためのファイルパスが以下の変数「BASE_DIR」に設定されていること

# COMMAND ----------

BASE_DIR="/user/motokazu.ishikawa@databricks.com/hail"

# COMMAND ----------

assert dbutils.fs.ls(BASE_DIR), "BASE_DIR needs to be a file path"

# COMMAND ----------

# MAGIC %md
# MAGIC ## hailをDatabricksで動作する場合の注意点
# MAGIC - DatabricksではSparkContextが独自に用意されるため、hailオブジェクトを起動する際に、引数として渡す必要があります
# MAGIC - Hailではグラフ描画にBokehライブラリを利用しており、Bokehでは描画にshow関数を利用しますが、showはDatabricksで使われている関数のため、別途htmlを作成して表示します

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備（インポート、データダウンロード等）

# COMMAND ----------

import hail as hl
from bokeh.embed import components, file_html
from bokeh.resources import CDN
hl.init(sc=sc, idempotent=True) 


# COMMAND ----------

from pprint import pprint
hl.plot.output_notebook()

# COMMAND ----------

hl.utils.get_1kg( BASE_DIR )
hl.import_vcf(f"{BASE_DIR}/1kg.vcf.bgz").write(f"{BASE_DIR}/1kg.mt", overwrite=True)
mt = hl.read_matrix_table(f"{BASE_DIR}/1kg.mt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの確認

# COMMAND ----------

mt.rows().select().show(5)

# COMMAND ----------

mt.row_key.show(5)

# COMMAND ----------

mt.s.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tableオブジェクト

# COMMAND ----------

table = (hl.import_table(f"{BASE_DIR}/1kg_annotations.txt", impute=True)
         .key_by('Sample'))

# COMMAND ----------

table.describe()

# COMMAND ----------

table.show(width=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 補足：Databricksのノートブックは可視化機能がビルトインされているので、 Spark DataFrameのデータに対してテーブルやグラフ描画をUIで操作可能です！

# COMMAND ----------

df = table.to_spark()
display( df )

# COMMAND ----------

print(mt.col.dtype)

# COMMAND ----------

mt = mt.annotate_cols(pheno = table[mt.s])

# COMMAND ----------

mt.col.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query functions and the Hail Expression Language

# COMMAND ----------

pprint(table.aggregate(hl.agg.counter(table.SuperPopulation)))

# COMMAND ----------

pprint(table.aggregate(hl.agg.stats(table.CaffeineConsumption)))

# COMMAND ----------

table.count()

# COMMAND ----------

mt.count_cols()

# COMMAND ----------

mt.aggregate_cols(hl.agg.counter(mt.pheno.SuperPopulation))

# COMMAND ----------

pprint(mt.aggregate_cols(hl.agg.stats(mt.pheno.CaffeineConsumption)))

# COMMAND ----------

snp_counts = mt.aggregate_rows(hl.agg.counter(hl.Struct(ref=mt.alleles[0], alt=mt.alleles[1])))
pprint(snp_counts)

# COMMAND ----------

from collections import Counter
counts = Counter(snp_counts)
counts.most_common()

# COMMAND ----------

p = hl.plot.histogram(mt.DP, range=(0,30), bins=30, title='DP Histogram', legend='DP')
displayHTML(file_html(p, CDN, "DP Histogram"))

# COMMAND ----------

mt.col.describe()

# COMMAND ----------

mt = hl.sample_qc(mt)
mt.col.describe()

# COMMAND ----------

p = hl.plot.histogram(mt.sample_qc.call_rate, range=(.88,1), legend='Call Rate')
displayHTML(file_html(p, CDN, "QC Metrics"))

# COMMAND ----------

p = hl.plot.histogram(mt.sample_qc.gq_stats.mean, range=(10,70), legend='Mean Sample GQ')
displayHTML(file_html(p, CDN, "Mean Sample GQ"))

# COMMAND ----------

p = hl.plot.scatter(mt.sample_qc.dp_stats.mean, mt.sample_qc.call_rate, xlabel='Mean DP', ylabel='Call Rate')
displayHTML(file_html(p, CDN, "Correlation"))

# COMMAND ----------

mt = mt.filter_cols((mt.sample_qc.dp_stats.mean >= 4) & (mt.sample_qc.call_rate >= 0.97))
print('After filter, %d/284 samples remain.' % mt.count_cols())

# COMMAND ----------

ab = mt.AD[1] / hl.sum(mt.AD)

filter_condition_ab = ((mt.GT.is_hom_ref() & (ab <= 0.1)) |
                        (mt.GT.is_het() & (ab >= 0.25) & (ab <= 0.75)) |
                        (mt.GT.is_hom_var() & (ab >= 0.9)))

fraction_filtered = mt.aggregate_entries(hl.agg.fraction(~filter_condition_ab))
print(f'Filtering {fraction_filtered * 100:.2f}% entries out of downstream analysis.')
mt = mt.filter_entries(filter_condition_ab)

# COMMAND ----------

mt = hl.variant_qc(mt)
mt.row.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's do a GWAS!

# COMMAND ----------

mt = mt.filter_rows(mt.variant_qc.AF[1] > 0.01)
mt = mt.filter_rows(mt.variant_qc.p_value_hwe > 1e-6)
print('Samples: %d  Variants: %d' % (mt.count_cols(), mt.count_rows()))

# COMMAND ----------

gwas = hl.linear_regression_rows(y=mt.pheno.CaffeineConsumption,
                                 x=mt.GT.n_alt_alleles(),
                                 covariates=[1.0])
gwas.row.describe()

# COMMAND ----------

p = hl.plot.manhattan(gwas.p_value)
displayHTML(file_html(p, CDN, "Manhattan Plot"))

# COMMAND ----------

p = hl.plot.qq(gwas.p_value)
displayHTML(file_html(p, CDN, "Q-Q Plot"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confounded !

# COMMAND ----------

eigenvalues, pcs, _ = hl.hwe_normalized_pca(mt.GT)
pprint(eigenvalues)

# COMMAND ----------

pcs.show(5, width=100)

# COMMAND ----------

mt = mt.annotate_cols(scores = pcs[mt.s].scores)
p = hl.plot.scatter(mt.scores[0],
                    mt.scores[1],
                    label=mt.pheno.SuperPopulation,
                    title='PCA', xlabel='PC1', ylabel='PC2')
displayHTML(file_html(p, CDN, "Populations"))

# COMMAND ----------

gwas = hl.linear_regression_rows(
    y=mt.pheno.CaffeineConsumption,
    x=mt.GT.n_alt_alleles(),
    covariates=[1.0, mt.pheno.isFemale, mt.scores[0], mt.scores[1], mt.scores[2]])
p = hl.plot.qq(gwas.p_value)
displayHTML(file_html(p, CDN, "Q-Q Plot"))

# COMMAND ----------

p = hl.plot.manhattan(gwas.p_value)
displayHTML(file_html(p, CDN, "Manhattan Plot"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rare variant analysis

# COMMAND ----------

entries = mt.entries()
results = (entries.group_by(pop = entries.pheno.SuperPopulation, chromosome = entries.locus.contig)
      .aggregate(n_het = hl.agg.count_where(entries.GT.is_het())))
results.show()

# COMMAND ----------

entries = entries.annotate(maf_bin = hl.if_else(entries.info.AF[0]<0.01, "< 1%",
                             hl.if_else(entries.info.AF[0]<0.05, "1%-5%", ">5%")))

results2 = (entries.group_by(af_bin = entries.maf_bin, purple_hair = entries.pheno.PurpleHair)
      .aggregate(mean_gq = hl.agg.stats(entries.GQ).mean,
                 mean_dp = hl.agg.stats(entries.DP).mean))
results2.show()
