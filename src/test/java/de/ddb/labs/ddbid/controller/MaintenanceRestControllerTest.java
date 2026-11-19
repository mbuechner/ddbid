package de.ddb.labs.ddbid.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import de.ddb.labs.ddbid.cronjob.ObjectsCronJob;
import de.ddb.labs.ddbid.cronjob.objects.Compare;
import de.ddb.labs.ddbid.cronjob.objects.Correct;
import de.ddb.labs.ddbid.cronjob.objects.Dump;
import de.ddb.labs.ddbid.cronjob.objects.Import;
import de.ddb.labs.ddbid.service.StatisticsService;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class MaintenanceRestControllerTest {

    @Test
    void schedulesMaintenanceOnlyFromAnExplicitMutationRequest() throws Exception {
        TaskScheduler scheduler = mock(TaskScheduler.class);
        var controller = new MaintenanceRestController(scheduler, mock(ObjectsCronJob.class), mock(Dump.class),
                mock(Compare.class), mock(Import.class), mock(Correct.class), mock(StatisticsService.class));
        var mvc = MockMvcBuilders.standaloneSetup(controller).build();

        mvc.perform(get("/maintenance/import").header("X-DDBID-Maintenance", "true")).andExpect(status().isMethodNotAllowed());
        mvc.perform(post("/maintenance/import")).andExpect(status().isNotFound());
        verifyNoInteractions(scheduler);

        mvc.perform(post("/maintenance/import").header("X-DDBID-Maintenance", "true")).andExpect(status().isAccepted());
        verify(scheduler).schedule(any(Runnable.class), any(Instant.class));
    }
}
