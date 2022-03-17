# GetNgram: Obtaining n-grams from PubMed using Hadoop

GetNgram package consists of GetNgram.java, GetNgramInputFormat.java, and GetNgramInputReader.java .

`$ hadoop jar GetNgram.jar jp.dbcls.hadoop.GetNgram [options] <Source Directory> <Target Directory>`

 - Options with default values
```
  -Dgetngram.consider.jinfo=false : whether or not considering journal information (Broad Subject Terms)
  -Dgetngram.case.sensitive=true  : whether or not considering case
  -Dgetngram.stem.porter=false    : whether or not stemming each word
  -Dgetngram.per.document=false   : whether or not processing per document (false means per sentence)
  -Dgetngram.ngram.minfreq=10     : minimum frequency of a n-gram in output
  -Dgetngram.ngram.size=2         : n of n-gram
```
This package is used to generate a dataset for inMeXes ( https://docman.dbcls.jp/im/ ).

The expected format of input text is as follows:
```
_pubmed_id=34120118:2021 Jun 11:Anatomy|Cell Biology
_ArticleTitle_
<s n="1">Biomaterial Strategies to Bolster Neural Stem Cell-Mediated Repair of the Central Nervous System.</s>
_AbstractText_
<s n="1">Stem cell therapies have the potential to not only repair, but to regenerate tissue of the central nervous system (CNS).</s>
...
<s n="9">This review will highlight the challenges associated with cell delivery in the CNS and the advances in biomaterial development and deployment for stem cell therapies necessary to bolster stem cell-mediated repair.</s>
_pubmed_id_end_
```

This format is line oriented, and the first line begins with `_pubmed_id=<PubMed ID>:<Date of Publish>:<Borad Subject Terms concatenated with |>`.
Then, the line only having `_ArticleTitle_` comes next followed by the title enclosed with the `<s>` tag.
This tag has one attribute of `n` denoting its sentence number.
After the last title sentence, the line only having `_AbstractText_` comes.
Then, each sentece of the abstract follows, and the last line has `_pubmed_id_end_` only.
Title and abstract have own sentence numbers.


# inMeXes生成用スクリプト群（Java編）

inMeXesは毎年暮れにリリースされるPubMedのbaselineセットを対象として、単語単位のバイグラムからデカ（10）グラムまでを取得して逐次検索可能にするサービスです。
そのために、上記PubMedデータを対象として、一行一文とする処理を行った後に、本レポジトリにあるHadoopアプリを用います。

そのほか、Hadoopを用いてPubMedを対象としたいくつかの処理アプリもこのレポジトリに含まれています。
