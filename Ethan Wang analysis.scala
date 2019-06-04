// Tasks that I must complete for my Project
// 1. Upload my csv files to hdfs
// 2. create a rdd that can read the csv files on hdfs.
// 3. Check what I have written down on R to find the codes that I need to find. 
// 4. Find some interesting analysis with the rdd. 
// 5a. Get the count for all the deaths for certain codes by year. completed with year
// 5b. Get the count for all deaths in total with overdose deaths. 
// 5c. Get the count for all deaths 
// Identify most common types of substance abuse related deaths A: X42 and X44 seem the most common.
// Identify current trends in substance abuse related deaths between age, ethnicity, sex and education levels.
// Identify any positive and negative trends in substance abuse related deaths in the United States.Mostly positive trends. Seems like accidental overdoes are going up. 


// hdfs dfs -ls /user/wang7644/mortality/*.csv

// The above command lists all the csv files in the mortality folder

val mortality = sc.textFile("/user/wang7664/mortality/*.csv")

// This loads all the csv files in mortality folder.

val mortalityhead = mortality.map(line => line.split(",")).map(f => f(0))


val current_data_year: Int = 16 
val ic10code: Int = 23
val drug_code: Int = 24
val gender: Int = 5
val age: Int = 11
val race: Int = 74
val edu: Int = 2

//this method works to read the csv without a header.
val mortality = sc.textFile("/user/wang7664/mortality/*.csv")
//this references the first row
val header = mortality.first

//this function filters out the first row. Takes out 10 but leaves 1 which I suspsect does
//have a exact macth to header creates a array string int format
val mortalityrdd = mortality.mapPartitions{x => x.filter(_ != header)}
//this function finds the errant header
val data = mortalityrdd.filter(s => !(s.contains("resident_status")))

// this method is similar to mortality rdd above but instead does array char int format
// val mortalityrdd = mortality.filter(x=>x != header)

//splits by comma 
val cleandata = data.map(line => line.split(","))

// this creates a rdd of resident_status data. 
val resident = cleandata.map(x=>x(0)).map{case(x)=>(x,1)}.reduceByKey(_+_,1)

//this above program works and provides csv data without headers. However...
//it seems to have converted all values to string. Which I think is fine. 

val year = cleandata.map(x=>x(current_data_year)).map{case(x)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

// this pulls all drug related deaths per year including codes:"420" ,"425","433","443","454"

val drugdeaths = cleandata.map(x=>(x(current_data_year),x(drug_code))).filter{case(x,y)=>y.contains("420")||y.contains("425")||y.contains("433")||y.contains("443")||y.contains("454")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)


val drugdeaths = cleandata.map(x=>(x(current_data_year),x(drug_code))).filter{case(x,y)=>y.contains("420")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

// The series of code here were to help me identify which paticular code was more predominant. However I realized that I could get more specific by researching the ICD-10 Code
val drugdeaths420 = cleandata.map(x=>(x(current_data_year),x(drug_code))).filter{case(x,y)=>y.contains("420")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)
val drugdeaths425 = cleandata.map(x=>(x(current_data_year),x(drug_code))).filter{case(x,y)=>y.contains("425")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)
val drugdeaths433 = cleandata.map(x=>(x(current_data_year),x(drug_code))).filter{case(x,y)=>y.contains("433")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)
val drugdeaths443 = cleandata.map(x=>(x(current_data_year),x(drug_code))).filter{case(x,y)=>y.contains("443")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)
val drugdeaths454 = cleandata.map(x=>(x(current_data_year),x(drug_code))).filter{case(x,y)=>y.contains("454")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val drugdeathsx42 = cleandata.map(x=>(x(current_data_year),x(ic10code))).filter{case(x,y)=>y.contains("X42")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)
//X42 seems to be the most common currently
val drugdeathsx41 = cleandata.map(x=>(x(current_data_year),x(ic10code))).filter{case(x,y)=>y.contains("X41")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)


val drugdeathsx44 = cleandata.map(x=>(x(current_data_year),x(ic10code))).filter{case(x,y)=>y.contains("X44")}.map{case(x,y)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

// this compares the deaths by year for drug related deaths between gender. We can see that more men die to drug related deaths by 7-13 thousand more a year than women.  
val drugdeathsgenderf = cleandata.map(x=>(x(current_data_year),x(gender),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("F")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val drugdeathsgenderm = cleandata.map(x=>(x(current_data_year),x(gender),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("M")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val joindrugdeathsgender = drugdeathsgenderm.join(drugdeathsgenderf)
val resultdrugdeathsgender = joindrugdeathsgender.mapValues(x => x._1 + x._2).sortByKey(ascending = true)

// This compares the difference between different age groups by five years. 


val drugdeathsage = cleandata.map(x=>(x(age),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2012")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val drugdeathsage2005 = cleandata.map(x=>(x(age),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2005")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val drugdeathsage2010 = cleandata.map(x=>(x(age),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2010")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val drugdeathsage2015 = cleandata.map(x=>(x(age),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2015")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val join2015and2010 =drugdeathsage2015.join(drugdeathsage2010)

val fiveyears = join2015and2010.mapValues(x => x._1-x._2).sortByKey(ascending = true)


// This compares the different ehtinic groups in relation to drug deaths by five years. 




val drugdeathsrace2005 = cleandata.map(x=>(x(race),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2005")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val drugdeathsrace2010 = cleandata.map(x=>(x(race),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2010")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val drugdeathsrace2015 = cleandata.map(x=>(x(race),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2015")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val join2015and2010race = drugdeathsrace2015.join(drugdeathsrace2010)

val fiveyearsrace = join2015and2010race.mapValues(x => x._1-x._2).sortByKey(ascending = true)

// This compares the different education deaths of 2010 to 2015


val drugdeathsedu2005 = cleandata.map(x=>(x(edu),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2005")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val drugdeathsedu2010 = cleandata.map(x=>(x(edu),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2010")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val drugdeathsedu2015 = cleandata.map(x=>(x(edu),x(current_data_year),x(drug_code))).filter{case(x,y,z)=>z.contains("420")||z.contains("425")||z.contains("433")||z.contains("443")||z.contains("454")}.filter{case(x,y,z)=>y.contains("2015")}.map{case(x,y,z)=>(x,1)}.reduceByKey(_+_,1).sortByKey(ascending = true)

val join2015and2010edu = drugdeathsedu2015.join(drugdeathsedu2010)

val fiveyearsedu = join2015and2010edu.mapValues(x => x._1-x._2).sortByKey(ascending = true)







