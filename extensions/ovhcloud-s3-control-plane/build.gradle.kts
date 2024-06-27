plugins {
    `java-library`
    id("org.spdx.sbom") version "0.8.0"
}

val edcGroup: String by project
val edcVersion: String by project
val metaModelVersion: String by project
val extensionsGroup: String by project
val extensionsVersion: String by project
val minIOVersion: String by project

val junitVersion: String by project
val mockitoVersion: String by project
val assertjVersion: String by project

repositories {
    mavenCentral()
}

dependencies {
    api("${edcGroup}:runtime-metamodel:${metaModelVersion}")

    implementation(project(":extensions:ovhcloud-s3-core"))
    implementation("${edcGroup}:transfer-spi:${edcVersion}")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:${assertjVersion}")
    testImplementation("${edcGroup}:json-lib:${edcVersion}")
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
