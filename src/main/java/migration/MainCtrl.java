package migration;
import migration.service.MigrationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

@SpringBootApplication
public class MainCtrl {
    private static final Logger logger = LogManager.getLogger(MainCtrl.class);

    public static void main(String args[]) {
        SpringApplication.run(MainCtrl.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            if (System.getProperty("config") != null && Boolean.parseBoolean(System.getProperty("export"))) {
                logger.info("Use user config " + System.getProperty("config"));
                try {
                    File file = new File(System.getProperty("config"));
                    if (file.exists()) {
                        FileInputStream inputStream = new FileInputStream(file);
                        Properties props = new Properties();
                        props.load(inputStream);

                        String fileName = props.getProperty("FILE");
                        String eshost = props.getProperty("ES_HOST");
                        //String buket = props.getProperty("S3_BUKET");
                        Long tenantId = Long.parseLong(props.getProperty("TENANTID"));

                        logger.info("going to export data fileName= {}, ESHOST={}, tenantId={}", fileName, eshost, tenantId);

                        MigrationService loadData = new MigrationService();
                        loadData.exportData(fileName, tenantId, eshost);
                    } else {
                        logger.info("cannot find file {}", System.getProperty("config"));
                    }
                } catch (IOException e) {
                    logger.error("file load fail ", e);
                }
            } else if (System.getProperty("config") != null && Boolean.parseBoolean(System.getProperty("export"))) {
                File file = new File(System.getProperty("config"));
                if (!file.exists()) {
                    logger.error("file not exist.. {}", file);
                }

                FileInputStream inputStream = new FileInputStream(file);
                Properties props = new Properties();
                props.load(inputStream);

                String fileName = props.getProperty("FILE");
                String eshost = props.getProperty("ES_HOST");
                MigrationService loadData = new MigrationService();
                loadData.importData(eshost, fileName);
            } else {
                logger.error("cannot get properties {}",System.getProperty("config") + System.getProperty("export"));
                System.exit(1);
            }

            System.exit(0);
        };
    }
}
