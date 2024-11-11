plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation ("org.apache.spark:spark-sql_2.12:3.0.0")
    implementation ("org.apache.spark:spark-hive_2.12:3.0.0")
    implementation ("org.apache.spark:spark-core_2.12:3.0.0")
    implementation ("io.delta:delta-core_2.12:0.7.0")
    implementation ("io.delta:delta-spark_2.12:0.7.0")
    implementation ("org.apache.spark:spark-hive-thriftserver_2.12:3.0.0")
}

tasks.test {
    useJUnitPlatform()
}