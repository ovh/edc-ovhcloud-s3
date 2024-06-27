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

dependencies {
    api("${edcGroup}:runtime-metamodel:${metaModelVersion}")
    implementation("${edcGroup}:transfer-spi:${edcVersion}")
    implementation("${edcGroup}:validator-spi:${edcVersion}")
    implementation("io.minio:minio:${minIOVersion}")

    testImplementation("org.junit.jupiter:junit-jupiter-api:${junitVersion}")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:${junitVersion}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${junitVersion}")
    testImplementation("org.mockito:mockito-core:${mockitoVersion}")
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
