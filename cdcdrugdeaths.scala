//eclipse examples

//Big Data Architecture project

import org.apache.spark.{SparkConf, SparkContext}

object sparkcdc {
  
  val current_data_year: Int = 16 
  val ic10code: Int = 23
  val drug_code: Int = 24
  val gender: Int = 5
  val age: Int = 11
  val race: Int = 74
  val edu: Int = 2

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("sparkcdc")
    val sc = new SparkContext(sparkConf)

    val mortality = sc.textFile("/user/wang7664/mortality/*.csv")
    
    val header = mortality.first
    // this removes the header and also removes a erroneous header 
    val cleandata = mortality.mapPartitions{x => x.filter(_!= header)}
      .filter(s => !(s.contains("resident_status")))
      .map(line => line.split(","))

    // this pulls all drug related deaths per year including codes:"420" ,"425","433","443","454"
    val drugdeaths = cleandata
      .map(x=>(x(current_data_year),x(drug_code)))
      .filter{case(x,y)=>y.contains("420")||y.contains("425")||y.contains("433")||y.contains("443")||y.contains("454")}
      .map{case(x,y)=>(x,1)}
      .reduceByKey(_+_,1).sortByKey(ascending = true)
      .saveAsTextFile("drugdeathsbyyearout")
    
    // this pulls all drug deaths with death code x42 by each year in 2005-2015
    val drugdeathsx42 = cleandata
      .map(x=>(x(current_data_year),x(ic10code)))
      .filter{case(x,y)=>y.contains("X42")}
      .map{case(x,y)=>(x,1)}
      .reduceByKey(_+_,1)
      .sortByKey(ascending = true)
      .saveAsTextFile("x42deathsbyyearout")

    // this pulls all drug deaths with death code x44 by each year in 2005-2015
    val drugdeathsx44 = cleandata
      .map(x=>(x(current_data_year),x(ic10code)))
      .filter{case(x,y)=>y.contains("X44")}
      .map{case(x,y)=>(x,1)}
      .reduceByKey(_+_,1)
      .sortByKey(ascending = true)
      .saveAsTextFile("x44deathsbyyearout")

    // this pulls all female American drug deaths with all drug codes by each year in 2005-2015
    val drugdeathsgenderf = cleandata
      .map(x=>(x(current_data_year),x(gender),x(drug_code)))
      .filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}
      .filter{case(x,y,z)=>y.contains("F")}
      .map{case(x,y,z)=>(x,1)}
      .reduceByKey(_+_,1).sortByKey(ascending = true)

    // this pulls all male American drug deaths with all drug codes by each year in 2005-2015
    val drugdeathsgenderm = cleandata
      .map(x=>(x(current_data_year),x(gender),x(drug_code)))
      .filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}
      .filter{case(x,y,z)=>y.contains("M")}
      .map{case(x,y,z)=>(x,1)}
      .reduceByKey(_+_,1).sortByKey(ascending = true)

    // this compares the differences of male and female drug related deaths by each year in 2005-2015
    val genderdrugdeathscomparison = drugdeathsgenderm
      .join(drugdeathsgenderf)
      .mapValues(x => x._1 - x._2)
      .sortByKey(ascending = true)
      .saveAsTextFile("genderdrugdeathscomparisonbyyearout")

    // this pulls all the different age bins of related drug deaths for the year 2010
    val drugdeathsage2010 = cleandata
      .map(x=>(x(age),x(current_data_year),x(drug_code)))
      .filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}
      .filter{case(x,y,z)=>y.contains("2010")}
      .map{case(x,y,z)=>(x,1)}
      .reduceByKey(_+_,1)
      .sortByKey(ascending = true)

    // this pulls all the different age bins of related drug deaths for the year 2015
    val drugdeathsage2015 = cleandata
      .map(x=>(x(age),x(current_data_year),x(drug_code)))
      .filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}
      .filter{case(x,y,z)=>y.contains("2015")}
      .map{case(x,y,z)=>(x,1)}
      .reduceByKey(_+_,1)
      .sortByKey(ascending = true)

    // this compares the deaths of different age groups from the year 2010 to the year 2015
    val age2015and2010comparison = drugdeathsage2015
      .join(drugdeathsage2010)
      .mapValues(x => x._1 - x._2)
      .sortByKey(ascending = true)
      .saveAsTextFile("age2015and2010comparisonout")

    // this shows the different ethnic groups that died due to drug related reasons in 2010
    val drugdeathsrace2010 = cleandata
      .map(x=>(x(race),x(current_data_year),x(drug_code)))
      .filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}
      .filter{case(x,y,z)=>y.contains("2010")}
      .map{case(x,y,z)=>(x,1)}
      .reduceByKey(_+_,1)
      .sortByKey(ascending = true)

    // this shows the different ethnic groups that died due to drug related reasons in 2015
    val drugdeathsrace2015 = cleandata
      .map(x=>(x(race),x(current_data_year),x(drug_code)))
      .filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}
      .filter{case(x,y,z)=>y.contains("2015")}
      .map{case(x,y,z)=>(x,1)}
      .reduceByKey(_+_,1)
      .sortByKey(ascending = true)

    // this compares the deaths of different ethnic groups from the year 2010 to the year 2015
    val join2015and2010race = drugdeathsrace2015
      .join(drugdeathsrace2010)
      .mapValues(x => x._1-x._2)
      .sortByKey(ascending = true)
      .saveAsTextFile("race2015and2010comparisonout")

    //This shows the different education groups that died due to drug related reasons in 2010
    val drugdeathsedu2010 = cleandata
      .map(x=>(x(edu),x(current_data_year),x(drug_code)))
      .filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}
      .filter{case(x,y,z)=>y.contains("2010")}
      .map{case(x,y,z)=>(x,1)}
      .reduceByKey(_+_,1)
      .sortByKey(ascending = true)

    //This shows the different education groups that died due to drug related reasons in 2015
    val drugdeathsedu2015 = cleandata
      .map(x=>(x(edu),x(current_data_year),x(drug_code)))
      .filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}
      .filter{case(x,y,z)=>y.contains("2015")}
      .map{case(x,y,z)=>(x,1)}
      .reduceByKey(_+_,1)
      .sortByKey(ascending = true)      

    //this compares the different education groups drug deaths from the year 2010 to the year 2015
    val join2015and2010edu = drugdeathsedu2015
      .join(drugdeathsedu2010)
      .mapValues(x => x._1-x._2)
      .sortByKey(ascending = true)
      .saveAsTextFile("edu2015and2010comparisonout")
  }
}
