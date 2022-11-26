plugins {
    signing
    id("java-library")
    id("maven-publish")
}

group = "dev.twelveoclock"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    // FastUtil
    api("it.unimi.dsi:fastutil:8.5.9")

    // Lombok
    compileOnly("org.projectlombok:lombok:1.18.24")
    annotationProcessor("org.projectlombok:lombok:1.18.24")
    testCompileOnly("org.projectlombok:lombok:1.18.24")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.24")

    // JUnit Jupiter
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.0")
}


tasks {
    test {
        useJUnitPlatform()
    }
}

publishing {
    repositories {
        maven {
            name = "12oclockDev"
            url = uri("https://maven.12oclock.dev/releases")
            credentials(PasswordCredentials::class)
            authentication {
                create<BasicAuthentication>("basic")
            }
        }
    }
    publications {
        create<MavenPublication>("maven") {
            groupId = "dev.twelveoclock"
            artifactId = "fastutil-concurrent"
            version = "1.0.5"
            from(components["java"])
        }
    }
}