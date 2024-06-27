plugins {
    `java-library`
    id("org.spdx.sbom") version "0.8.0"
}

val edcGroup: String by project
val edcVersion: String by project
val metaModelVersion: String by project
val extensionsGroup: String by project
val extensionsVersion: String by project
val minIOGroup: String by project
val minIOVersion: String by project

val junitGroup: String by project
val junitVersion: String by project
val mockitoGroup: String by project
val mockitoVersion: String by project

repositories {
    mavenCentral()
}

dependencies {
    api("${edcGroup}:runtime-metamodel:${metaModelVersion}")

    implementation(project(":extensions:ovhcloud-s3-core"))
    implementation("${edcGroup}:transfer-spi:${edcVersion}")
    implementation("${edcGroup}:data-plane-util:${edcVersion}")
    implementation("${edcGroup}:data-plane-core:${edcVersion}")
    implementation("${edcGroup}:http:${edcVersion}")
    implementation("${edcGroup}:validator-spi:${edcVersion}")
    implementation("${minIOGroup}:minio:${minIOVersion}")

    testImplementation("${junitGroup}:junit-jupiter-api:${junitVersion}")
    testImplementation("${junitGroup}:junit-jupiter-engine:${junitVersion}")
    testImplementation("${junitGroup}:junit-jupiter-params:${junitVersion}")
    testImplementation("${mockitoGroup}:mockito-core:${mockitoVersion}")

}

java {
    withJavadocJar()
    withSourcesJar()
}

spdxSbom {
    targets {
        create("release") {
        }
    }
}

tasks.test {
    useJUnitPlatform()
}
