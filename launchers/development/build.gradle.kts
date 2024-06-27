plugins {
    `java-library`
    id("application")
    id("com.github.johnrengelman.shadow") version "7.0.0"
}

repositories {
    maven {// while runtime-metamodel dependency is still a snapshot
        url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
    }
    mavenCentral()
    mavenLocal()
}

val edcGroup: String by project
val edcVersion: String by project

dependencies {
    runtimeOnly("${edcGroup}:boot:${edcVersion}")
    runtimeOnly("${edcGroup}:connector-core:${edcVersion}")
    runtimeOnly("${edcGroup}:configuration-filesystem:${edcVersion}")

    runtimeOnly("${edcGroup}:dsp:${edcVersion}")
    runtimeOnly("${edcGroup}:http:${edcVersion}")
    runtimeOnly("${edcGroup}:iam-mock:${edcVersion}")

    runtimeOnly("${edcGroup}:control-plane-core:${edcVersion}")
    runtimeOnly("${edcGroup}:control-plane-api:${edcVersion}")
    runtimeOnly("${edcGroup}:control-plane-api-client:${edcVersion}")

    runtimeOnly("$edcGroup:management-api:$edcVersion")
    runtimeOnly("${edcGroup}:api-observability:${edcVersion}")

    runtimeOnly("${edcGroup}:data-plane-core:${edcVersion}")
    runtimeOnly("${edcGroup}:data-plane-client:${edcVersion}")
    runtimeOnly("${edcGroup}:data-plane-selector-core:${edcVersion}")
    runtimeOnly("${edcGroup}:data-plane-selector-api:${edcVersion}")
    runtimeOnly("${edcGroup}:transfer-data-plane:${edcVersion}")

    runtimeOnly(project(":extensions:ovhcloud-s3-core"))
    runtimeOnly(project(":extensions:ovhcloud-s3-control-plane"))
    runtimeOnly(project(":extensions:ovhcloud-s3-data-plane"))

}

application {
    mainClass.set("org.eclipse.edc.boot.system.runtime.BaseRuntime")
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    exclude("**/pom.properties", "**/pom.xm")
    mergeServiceFiles()
    archiveFileName.set("dataspace-connector.jar")
}
