package io.cdap.cdap.internal.app.worker.system;

import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import java.net.URI;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

public class SystemWorkerTwillApplication implements TwillApplication {

  public static final String NAME = "system.worker";

  private final URI cConfFileURI;
  private final URI hConfFileURI;
  private final ResourceSpecification artifactLocalizerResourceSpec;
  private final ResourceSpecification systemWorkerResourceSpec;


  public SystemWorkerTwillApplication(URI cConfFileURI, URI hConfFileURI,
      ResourceSpecification systemWorkerResourceSpec,
      ResourceSpecification artifactLocalizerResourceSpec) {
    this.cConfFileURI = cConfFileURI;
    this.hConfFileURI = hConfFileURI;
    this.systemWorkerResourceSpec = systemWorkerResourceSpec;
    this.artifactLocalizerResourceSpec = artifactLocalizerResourceSpec;
  }

  @Override
  public TwillSpecification configure() {
    return TwillSpecification.Builder.with()
        .setName(NAME)
        .withRunnable()
        .add(new SystemWorkerTwillRunnable("cConf.xml", "hConf.xml"), systemWorkerResourceSpec)
        .withLocalFiles()
        .add("cConf.xml", cConfFileURI)
        .add("hConf.xml", hConfFileURI)
        .apply()
        .add(new ArtifactLocalizerTwillRunnable("cConf.xml", "hConf.xml"),
            artifactLocalizerResourceSpec)
        .withLocalFiles()
        .add("cConf.xml", cConfFileURI)
        .add("hConf.xml", hConfFileURI)
        .apply()
        .anyOrder()
        .build();
  }
}
