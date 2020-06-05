
package install.script
import com.peterservice.ufm.settings.Settings
import com.peterservice.ufm.admintools.command.script.ScriptBeanFactory
import com.peterservice.ufm.loader.script.IClientExecutor
import com.peterservice.ufm.cachebusinesslayer.builder.client.common.ClientBuilderContainer
import com.peterservice.ufm.model.postpone.PostponedEvent
import org.slf4j.Logger
import java.text.SimpleDateFormat
import org.slf4j.LoggerFactory
import com.peterservice.ufm.model.telecom.Client
import com.peterservice.ufm.model.telecom.Service
import com.peterservice.ufm.model.telecom.SubsPack
import com.peterservice.ufm.model.telecom.Subscriber
import com.peterservice.ufm.model.common.Constants
import com.peterservice.ufm.model.telecom.BisCfgConstants
import com.peterservice.ufm.model.sanction.SanctionExec
import java.util.*
import com.peterservice.ufm.model.catalog.Parameters
import com.peterservice.ufm.settings.Settings
import com.peterservice.ufm.settings.SettingsManager


public class LC_TO_DEL implements IClientExecutor {
    private final SettingsManager settingsManager = ScriptBeanFactory.getBean("settingsManager", SettingsManager.class)
	
    String newResFile = "../temp/newResFile.txt"
    def newFileRes = new File(newResFile)
    String header = "\n CLNT_ID;" + "SUBS_ID;" + "RTPL;" + "RTST;" + "rtstS;" + "SRV_ACTIVITY_DATE;" + "RTST_ACTIVITY_DATE;" + "RTPL_ACTIVITY_DATE;" + "ACTIVITY_DATE;" + "PACK_ACTIVITY_DATE;";
    private static final Logger LOGGER = LoggerFactory.getLogger(LC_TO_DEL.class);
    private static final String OUTPUT_DATE_FORMAT_STRING = "dd.MM.yyyy' 'HH:mm:ss"    //   yyyy-MM-dd'T'HH:mm:ssZZ"
    SimpleDateFormat dateFormat = new SimpleDateFormat(OUTPUT_DATE_FORMAT_STRING);
    Settings settings = settingsManager.getSettings()
    Parameters parameters = settings.getParameters()

    @Override
    void scriptExecute(ClientBuilderContainer container) {
	Client client = container.getClient()
        String reason = "NULL"
        boolean save_number
	boolean no_block
	boolean has61001
        String migrStatus;
        int subsMarker

        if (client.getStringAttribute('MGR_STATUS')) { // проверяем есть ли атрибут миграции
            migrStatus = client.getStringAttribute('MGR_STATUS')
//            LOGGER.info("CLNT: {} balance",balSpent)
        }
        else (migrStatus='FINISH_IN') // если нет атрибута то присваем что все равно мигрировал
        if (BisCfgConstants.CLIENT_STATUS_CLOSE != client.getLongAttribute(Constants.CLIENT_STATUS) &&
        (!parameters.getListLongValue(BisCfgConstants.TECHNOLOGY_CLIENT_IDS, client.getDataSourceId()).contains(Long.parseLong(client.getObjectId())))) { 
	   if (migrStatus == 'FINISH_IN') { //проверяем мигрировал ли
                for (Subscriber subs : client.getSubscriberProxy().values) {
					subsMarker = 0
					save_number = false
					no_block = false
					has61001 = false
					reason = "sub is closed"
					if (subs.getLongAttribute(Constants.SUBS_STATUS) == 2) {
						subsMarker = 1
						for (PostponedEvent event : client.getPostponedEvents()) {
							if (event.getDiscr() && event.context.objectId == subs.getObjectId()) { //существуют ли постпоны вообще
								if (event.getDiscr() == 'SAVE_NUMBER_APPLY' || event.getDiscr() == 'LC_TO_DELETED') {   
									reason = 'has postpone'
									subsMarker = 0
								}
							}
						}
						if (subsMarker != 0) {							
							for (SanctionExec exec : client.getSanctionExecProxy().getSanctionExecs()) {
								if (exec.getSubscriberId() == subs.getObjectId() && exec.getSancId() == 61001) {
									has61001 = true
									if (![601,602,609,616].contains(exec.getStatus())){
										subsMarker = 0
										reason = "61001 not in 601,602,609,616"
									}
								}
							}
						}
						if (has61001 == false) {
							subsMarker = 0
							reason = "61001 is empty"
						}
						if (subsMarker != 0){
							for (Service serv : subs.getBisServiceProxy().getObjectMap().values()) {                                
								if (serv.getObjectId() == '186') {
									save_number = true;
								};
							}
							if (save_number == true) {
								subsMarker = 0
								reason = "srv 186 exist"
							}
						}
						if (subsMarker != 0){
							reason = "unknown"
							for (SubsPack pack : subs.getSubsPackProxy().getObjectMap().values()) {                                
								if (['2216','9423','9424','9425','9426'].contains(pack.getObjectId())) {
									no_block = true;
								};
							}
							if (no_block == true) {
								subsMarker = 0
								reason = "has no-auto-remove pack"
							}
						}
					}                    
                    //String textwithMaxDate = client.getObjectId() + ";" + subs.getObjectId() + ";" + subs.getLongAttribute(Constants.RATE_PLAN_ID) + ";" + subs.getLongAttribute(Constants.SUBS_RT_STATUS) + ";" + maxDateActivity;
                    String text = "\n" + client.getObjectId() + ";" + subs.getObjectId() + ";" + subs.getLongAttribute(Constants.RATE_PLAN_ID) + ";" + subs.getLongAttribute(Constants.SUBS_RT_STATUS) + reason;
                    if (subsMarker == 1) {
                        //LOGGER.info("info: {}", textwithMaxDate)// выведет только максимальную дату активности в консоль
                        newFileRes.append("\n" +client.getObjectId())
                        newFileRes.append(text) //Пишет только проблемных в файл
                    } else {
                        newFileRes.append(text) //Пишет все неподходящие резы в файл
                        //LOGGER.info("info: {} ++++",text)
                    }
                }
            }
        }
    }
}
