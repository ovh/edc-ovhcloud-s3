import com.github.jk1.license.render.ReportRenderer
import com.github.jk1.license.render.JsonReportRenderer

plugins {
    `java-library`
    id("com.github.jk1.dependency-license-report") version "2.8"
}

repositories {
    mavenLocal()
    mavenCentral()
}

licenseReport {
    
    renderers = arrayOf<ReportRenderer>(JsonReportRenderer("licenses.json"))
    
}