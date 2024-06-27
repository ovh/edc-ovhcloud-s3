plugins {
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
    `maven-publish`
    `signing`
}


val artifactIds = mapOf(
    "ovhcloud-s3-control-plane" to "control-plane-s3",
    "ovhcloud-s3-core" to "core-s3",
    "ovhcloud-s3-data-plane" to "data-plane-s3"
)

val v = System.getenv("EDC_EXTENSIONS_VERSION")
val u = System.getenv("OSSRH_USERNAME")
val p = System.getenv("OSSRH_PASSWORD")
val h = System.getenv("OSSRH_HOSTNAME")

val signingKey = System.getenv("SIGNING_KEY")
val signingKeyPassphrase = System.getenv("SIGNING_KEY_PASSPHRASE")

group = "com.ovhcloud.edc"

subprojects {
    apply(plugin = "maven-publish")
    apply(plugin = "java")
    apply(plugin = "signing")

    if (name in listOf("ovhcloud-s3-control-plane","ovhcloud-s3-data-plane","ovhcloud-s3-core")) {
        publishing {
            publications {
                create<MavenPublication>("${project.name}") {
                    groupId = "com.ovhcloud.edc"
                    artifactId = artifactIds[project.name] ?: project.name
                    version = v
                    from(components["java"])
                    pom {
                        name.set("${artifactId}")
                        description.set("Eclipse Dataspace Connector (EDC) extension component: ${artifactId}")
                        url.set("https://projects.eclipse.org/projects/technology.edc")
                        licenses {
                            license {
                                name.set("The Apache License, Version 2.0")
                                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                            }
                        }
                        developers {
                            developer {
                                name.set("Olivier Caudron")
                                email.set("olivier.caudron@ovhcloud.com")
                                organization.set("OVHcloud")
                                organizationUrl.set("https://www.ovhcloud.com")
                            }
                            developer {
                                name.set("David Drugeon-Hamon")
                                email.set("david.drugeon-hamon@wescale.fr")
                                organization.set("wescale")
                                organizationUrl.set("https://www.wescale.fr/")
                            }
                            developer {
                                name.set("Jean-Raymond Sue")
                                email.set("jean-raymond.sue@ovhcloud.com")
                                organization.set("OVHcloud")
                                organizationUrl.set("https://www.ovhcloud.com")
                            }
                            developer {
                                name.set("Pierre-Henri Symoneaux")
                                email.set("pierre-henri.symoneaux@ovhcloud.com")
                                organization.set("OVHcloud")
                                organizationUrl.set("https://www.ovhcloud.com")
                            }
                        }
                        scm {
                            connection.set("scm:git:git@github.com:ovh/edc-ovhcloud-s3.git")
                            developerConnection.set("scm:git:ssh://github.com:ovh/edc-ovhcloud-s3.git")
                            url.set("https://github.com/ovh/edc-ovhcloud-s3/tree/main")
                        }
                    }
                }
            }
        }
        
        signing {
            useInMemoryPgpKeys(signingKey, signingKeyPassphrase)
            sign(publishing.publications["${project.name}"])
        }
    }
}

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://${h}/service/local/"))
            snapshotRepositoryUrl.set(uri("https://${h}/content/repositories/snapshots/"))
            username.set(u)
            password.set(p)
        }
    }
}

