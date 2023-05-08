
global with sharing class AutomaticAccDataDeletionBatch implements Database.Batchable<sObject>,Schedulable{
   
    DPA_Status_AccDataDeletion__c DPA_FourPlus = DPA_Status_AccDataDeletion__c.getValues('FourPlus');
    string fourplus = DPA_FourPlus.DPA_Status__c;
   
    DPA_Status_AccDataDeletion__c DPA_Five = DPA_Status_AccDataDeletion__c.getValues('Five');
    string five = DPA_Five.DPA_Status__c;
   
    DPA_Status_AccDataDeletion__c DPA_FivePlus = DPA_Status_AccDataDeletion__c.getValues('FivePlus');
    string fivePlus = DPA_FivePlus.DPA_Status__c;
   
    DPA_Status_AccDataDeletion__c DPA_Six = DPA_Status_AccDataDeletion__c.getValues('SixPlus');
    string sixPlus = DPA_Six.DPA_Status__c;
   
    String executeQueryName;
   
    global AutomaticAccDataDeletionBatch(String executeQueryName){
        this.executeQueryName  = executeQueryName;
        system.debug('executeQueryName'+executeQueryName);
    }
   
   
   
    global Database.QueryLocator start(Database.BatchableContext bc){
        string sQuery;
        /*String BMW_NFSCUNIT     ='DE-NFSC';
String BMW_NSCUNIT       ='DE-NSC';
String BMWUnit_BENSC    ='BE-NSC';
String BMWUnit_BENFSC    ='BE-NFSC';
String CONDITION        ='AND BMW_Unit__c IN('+'\'' + BMW_NFSCUNIT + '\''+',\'' + BMW_NSCUNIT+ '\')';
String CONDITIONFORSIX     ='AND BMW_Unit__c IN('+'\'' + BMW_NFSCUNIT + '\''+',\'' + BMW_NSCUNIT+ '\' '+',\'' + BMWUnit_BENSC+ '\' '+',\'' + BMWUnit_BENFSC+ '\')';*/
       
        //Added custom metadata type to restrict the deletion based on market as part of WHCRM-43305
        List<DPA_Deletion_Market__mdt> markets =[Select Id, DPA_Market__c from DPA_Deletion_Market__mdt];
        List<String> units = new List<String>();
        for(DPA_Deletion_Market__mdt varMarket : markets){
            units.addAll(varMarket.DPA_Market__c.split(','));
        }
        if(executeQueryName.equals('SixPlus')){
            sQuery = 'SELECT Id,Name,DPA_Status__c FROM Account Where DPA_Status__c =: sixPlus AND BMW_Unit__c in :units LIMIT 10000';
            system.debug(' SIX PLUS sQuery'+sQuery);
        }
        else{
            sQuery = 'SELECT Id,Name,DPA_Status__c FROM Account Where (DPA_Status__c =: fourplus OR DPA_Status__c =: five OR DPA_Status__c =: fivePlus) AND BMW_Unit__c in :units ';
            system.debug(' Other than SIX PLUS sQuery'+sQuery);
        }
        return Database.getQueryLocator(sQuery);
    }
    global void execute(SchedulableContext sc){
        List<Batch_Job_Size__mdt> jobSizes = [SELECT Batch_Size__c FROM Batch_Job_Size__mdt WHERE DeveloperName = 'AutomaticAccDataDeletionBatch'];
       
        Integer batchSize = 200;
        if(jobSizes != null && jobSizes.size() > 0) {
            if(jobSizes[0].Batch_Size__c != null && jobSizes[0].Batch_Size__c > 0) {
                batchSize = Integer.valueOf(jobSizes[0].Batch_Size__c);
            }
        }
       
        AutomaticAccDataDeletionBatch utilityBatch = new AutomaticAccDataDeletionBatch(executeQueryName);
        try {    
            Database.executeBatch(utilityBatch,batchSize);
            Logger.info('Account data deletion is completed successfully.');
        } catch (Exception e) {
            Logger.error('Error: The following error is thrown while executing the batch: ' + e.getMessage() + ' ' + e.getStackTraceString());
        }
        Logger.saveLog();
    }
    global void execute (Database.BatchableContext BC, list<Account> scope){
        try{
            set<Id> setAccId = new Set<Id>();
            Set<Id> setAccStatusSixPlus = new Set<Id>();
            system.debug('scope>>'+scope);
            if(!scope.isEmpty()){
                for(Account acc:scope){
                    if(acc.DPA_Status__c !='' && (acc.DPA_Status__c == fourplus || acc.DPA_Status__c == five || acc.DPA_Status__c == fivePlus || acc.DPA_Status__c == sixPlus)){
                        setAccId.add(acc.Id);
                        system.debug('setAccId: '+setAccId);
                    }
                    if(acc.DPA_Status__c !='' && acc.DPA_Status__c == sixPlus){
                        setAccStatusSixPlus.add(acc.Id);
                        system.debug('setAccStatusSixPlus: '+setAccStatusSixPlus);
                    }
                }
            }
           
            //Query Account to Account Relations to be deleted
            List<Account_Account_Relation__c> lstCustRel = [SELECT Id,Parent_Account__r.Id,Parent_Account__r.DPA_Status__c,
                                                            Related_Account__r.Id,Related_Account__r.DPA_Status__c
                                                            FROM Account_Account_Relation__c
                                                            WHERE (Parent_Account__r.Id in: setAccId
                                                                   OR Related_Account__r.Id in: setAccId )
                                                            ORDER BY Id];
           
            List<Account_Account_Relation__c>lstCustRelToBeDeleted = new List<Account_Account_Relation__c>();
            List<Account_Account_Relation__c>lstCustRelToBeHardDeleted = new List<Account_Account_Relation__c>();
           
            for(Account_Account_Relation__c accRel: lstCustRel){
                if(accRel.Parent_Account__r.DPA_Status__c == sixPlus){
                    lstCustRelToBeHardDeleted.add(accRel);
                }else{
                    lstCustRelToBeDeleted.add(accRel);
                }
            }
            if(!lstCustRelToBeDeleted.isEmpty()){
                Database.delete(lstCustRelToBeDeleted,false);
            }
            if(!lstCustRelToBeHardDeleted.isEmpty()){
                Database.delete(lstCustRelToBeHardDeleted,false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstCustRelToBeHardDeleted);
                }
                //Database.emptyRecycleBin(lstCustRelToBeHardDeleted);
                Logger.info('Account to Account Relation is deleted');
            }
           
            //Query Assets
            List<Asset> lstAsset = [SELECT Id,Owner__r.Id,Owner__r.DPA_Status__c,Keeper__r.Id,User__r.Id
                                    FROM Asset
                                    WHERE (Owner__r.Id in:setAccId
                                           OR Keeper__r.Id in:setAccId
                                           OR User__r.Id in:setAccId )
                                    ORDER BY Id];
           
            List<Asset> lstC2VRelToBeDeleted = new List<Asset>();
            List<AssetUserRelation__c> lstAURToBeDeleted = new List<AssetUserRelation__c>();
           
            //Query Asset User Relation record
            Map<Id,AssetUserRelation__c> mapAssetUserRelation = new Map<Id,AssetUserRelation__c>();
            system.debug(setAccId);
            system.debug('assets');
            system.debug(lstAsset);
            for(AssetUserRelation__c oAUR : [SELECT Id, Account__c, Asset__c, Start_Date__c, End_Date__c, Source_System__c, User_Change_Date__c,
                                             Account__r.TopDrive_Business_ID__c,Account__r.GCID__c,Account__r.BMW_Unit__c,createddate
                                             FROM AssetUserRelation__c WHERE Asset__c IN :lstAsset and Account__c NOT IN :setAccId ])
            {
                if(mapAssetUserRelation.containsKey(oAUR.Asset__c))
                {
                    //if there are already records in the map related to the asset, select the one with earliest start date
                    AssetUserRelation__c oAURTemp = mapAssetUserRelation.get(oAUR.Asset__c);
                    system.debug('oAUR.Start_Date__c>>'+oAUR.Start_Date__c);
                    system.debug('oAURTemp.Start_Date__c>>'+oAURTemp.Start_Date__c);
                    if (oAUR.Start_Date__c > oAURTemp.Start_Date__c) {
                        mapAssetUserRelation.put(oAUR.Asset__c, oAUR);
                    }
                   
                }
                else if(!mapAssetUserRelation.containsKey(oAUR.Asset__c))
                {
                    mapAssetUserRelation.put(oAUR.Asset__c, oAUR);
                }
            }
            system.debug(mapAssetUserRelation);
            for(Asset oAsset : lstAsset)
               
            {
                system.debug('oAsset>>'+oAsset);
                if(oAsset.Owner__c != null && setAccId.contains(oAsset.Owner__r.Id) && oAsset.Owner__r.DPA_Status__c != sixPlus )
                {
                    oAsset.Owner__c = null;
                    oAsset.Owner_Change_Date__c = null;
                    oAsset.Owner_End_Date__c = null;
                    oAsset.Source_System__c = null;
                    oAsset.Owner_Start_Date__c = null;
                }
               
                if(oAsset.Keeper__c != null && setAccId.contains(oAsset.Keeper__r.Id))
                {
                    oAsset.Keeper__c = null;
                    oAsset.Keeper_Change_Date__c = null;
                    oAsset.Keeper_End_Date__c = null;
                    oAsset.Keeper_Source_System__c = null;
                    oAsset.Keeper_Start_Date__c = null;
                }
               
                if(oAsset.User__c != null && (setAccId.contains(oAsset.User__r.Id)) && mapAssetUserRelation.containsKey(oAsset.Id))
                {
                    AssetUserRelation__c oAUR = mapAssetUserRelation.get(oAsset.Id);
                    oAsset.User__c = oAUR.Account__c;
                    oAsset.User_Change_Date__c = oAUR.User_Change_Date__c;
                    oAsset.User_End_Date__c = oAUR.End_Date__c;
                    oAsset.User_Source_System__c = oAUR.Source_System__c;
                    oAsset.User_Start_Date__c = oAUR.Start_Date__c;
                    lstAURToBeDeleted.add(oAUR);
                   
                }else if(oAsset.User__c != null && (setAccId.contains(oAsset.User__r.Id))){
                    oAsset.User__c = null;
                    oAsset.User_Change_Date__c = null;
                    oAsset.User_End_Date__c = null;
                    oAsset.User_Source_System__c = null;
                    oAsset.User_Start_Date__c = null;
                }
               
                lstC2VRelToBeDeleted.add(oAsset);
                system.debug('lstC2VRelToBeDeleted :' +lstC2VRelToBeDeleted);
            }
           
            if(!lstC2VRelToBeDeleted.isEmpty()){
                Database.update(lstC2VRelToBeDeleted, false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstC2VRelToBeDeleted);
                }
                Logger.info('Customer to Vehicle relation is deleted.');
            }
           
            //Query Asset User Relations to be deleted
            List<AssetUserRelation__c> lstAUR = [SELECT Id, Account__c, Account__r.Id,Account__r.DPA_Status__c
                                                 FROM AssetUserRelation__c
                                                 WHERE Account__r.Id in: setAccId
                                                 ORDER BY Id];
            List<AssetUserRelation__c> lstAURToBeHardDeleted = new List<AssetUserRelation__c>();
           
            For(AssetUserRelation__c aur : lstAUR){
                if(setAccId.contains(aur.Account__r.Id) && aur.Account__r.DPA_Status__c == sixPlus){
                    lstAURToBeHardDeleted.add(aur);
                }else{
                    lstAURToBeDeleted.add(aur);
                }
            }
            if(!lstAURToBeDeleted.isEmpty()){
                Database.delete(lstAURToBeDeleted, false);
            }
            if(!lstAURToBeHardDeleted.isEmpty()){
                Database.delete(lstAURToBeHardDeleted, false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstAURToBeHardDeleted);
                }
                //Database.emptyRecycleBin(lstAURToBeHardDeleted);
                Logger.info('Asset user relation is deleted.');
            }
            //Query OKU History
            list<OKU_History__c> lstOKUHistory = [Select Id, Account__c, Account__r.Id, Account__r.DPA_Status__c
                                                  from OKU_History__c
                                                  where Account__r.Id in: setAccId
                                                  ORDER BY Id];
            List <OKU_History__c> lstOKUHistToBeDeleted = new List <OKU_History__c>();
            List<OKU_History__c> lstOKUHistToBeHardDeleted = new List <OKU_History__c>();
           
            for(OKU_History__c oku : lstOKUHistory){
                if(setAccId.contains(oku.Account__r.Id) && oku.Account__r.DPA_Status__c == sixPlus){
                    lstOKUHistToBeHardDeleted.add(oku);
                }else{
                    lstOKUHistToBeDeleted.add(oku);
                }
            }
            if(!lstOKUHistToBeDeleted.isEmpty())
            {
                Database.delete(lstOKUHistToBeDeleted, false);
            }
            if(!lstOKUHistToBeHardDeleted.isEmpty())
            {
                Database.delete(lstOKUHistToBeHardDeleted, false);
                //Database.emptyRecycleBin(lstOKUHistToBeHardDeleted);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstOKUHistToBeHardDeleted);
                }
                Logger.info('OKU History is deleted.');
            }
            //Query INternet Profiles to be deleted
            List<Internet_Profile__c> lstIntProf = [SELECT Id, Account__c, Account__r.Id,Account__r.DPA_Status__c
                                                    FROM Internet_Profile__c
                                                    WHERE Account__r.Id in: setAccId
                                                    ORDER BY Id];
            List <Internet_Profile__c> lstIPToBeDeleted = new List <Internet_Profile__c>();
            List <Internet_Profile__c> lstIPToBeHardDeleted = new List <Internet_Profile__c>();
           
            for(Internet_Profile__c intProf : lstIntProf){
                if(setAccId.contains(intProf.Account__r.Id) && intProf.Account__r.DPA_Status__c == sixPlus){
                    lstIPToBeHardDeleted.add(intProf);
                }else if(setAccId.contains(intProf.Account__r.Id) && (intProf.Account__r.DPA_Status__c == five || intProf.Account__r.DPA_Status__c == fivePLus)){
                    lstIPToBeDeleted.add(intProf);
                }
            }            
            if(!lstIPToBeDeleted.isEmpty()){
                Database.delete(lstIPToBeDeleted, false);
            }
            if(!lstIPToBeHardDeleted.isEmpty()){
                Database.delete(lstIPToBeHardDeleted, false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstIPToBeHardDeleted);
                }
                //Database.emptyRecycleBin(lstIPToBeHardDeleted);
                Logger.info('Internet Profiles are deleted.');
            }
            //Query Cases to be deleted
            List<Case> lstCase = [SELECT Id, AccountID, Account.Id,Account.DPA_Status__c
                                  FROM Case WHERE Account.Id in: setAccId
                                  ORDER BY Id];
            List <Case> lstCaseToBeDeleted = new List <Case>();
            for(Case oCase: lstCase){
                if(setAccId.contains(oCase.Account.Id) && oCase.Account.DPA_Status__c == sixPlus) {
                    lstCaseToBeDeleted.add(oCase);
                }
            }
            if(!lstCaseToBeDeleted.isEmpty()){
                Database.delete(lstCaseToBeDeleted, false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstCaseToBeDeleted);
                }
                //Database.emptyRecycleBin(lstCaseToBeDeleted);
                Logger.info('Cases are deleted.');
            }
            //Query Account Dealer Relations
            List<Account_Dealer_Relation__c> lstAccDelRel = [SELECT Id,Related_Account__c,Related_Account__r.Id,Related_Account__r.DPA_Status__c
                                                             FROM Account_Dealer_Relation__c
                                                             WHERE Related_Account__r.Id in: setAccId
                                                             ORDER BY Id];
            List<Account_Dealer_Relation__c> lstAccDelRelToBeDeleted = new List <Account_Dealer_Relation__c>();
           
            for(Account_Dealer_Relation__c accDelRel : lstAccDelRel){
                if(setAccId.contains(accDelRel.Related_Account__r.Id) && accDelRel.Related_Account__r.DPA_Status__c == sixPlus) {
                    lstAccDelRelToBeDeleted.add(accDelRel);
                }
            }
            if(!lstAccDelRelToBeDeleted.isEmpty())
            {
                Database.delete(lstAccDelRelToBeDeleted, false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstAccDelRelToBeDeleted);
                }
                //Database.emptyRecycleBin(lstAccDelRelToBeDeleted);
                Logger.info('Account Delaer relations are deleted.');
            }
            //Query Files to delete
            List<ContentDocumentLink> lstFiles = new List<ContentDocumentLink>();
            List<Id> lstFilesToBeDeleted = new List<Id>();
            if(!setAccStatusSixPlus.isEmpty())
            {
                lstFiles = [SELECT Id,ContentDocumentId, LinkedEntityId
                            FROM ContentDocumentLink
                            where LinkedEntityId in: setAccStatusSixPlus
                            and LinkedEntity.Type='Account'
                            ORDER BY Id];
            }
            if(!lstFiles.isEmpty()){
                for(ContentDocumentLink contentDoc : lstFiles){
                    lstFilesToBeDeleted.add(contentDoc.ContentDocumentId);
                }
            }
            if(!lstFilesToBeDeleted.isEmpty()){
                Database.delete(lstFilesToBeDeleted,false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstFilesToBeDeleted);
                }
                //Database.emptyRecycleBin(lstFilesToBeDeleted);
                Logger.info('Files are deleted.');
            }
            //Query Requests to be deleted
            List<Request__c> lstRequest = [Select id,Account__r.Id,Account__r.DPA_Status__c from Request__c where Account__r.Id in: setAccId];
            List<Request__c>lstRequestToBeHardDeleted = new List<Request__c>();
            for(Request__c req : lstRequest){
                if(req.Account__r.DPA_Status__c == sixPlus){
                    lstRequestToBeHardDeleted.add(req);
                }
            }
            if(!lstRequestToBeHardDeleted.isEmpty()){
                Database.delete(lstRequestToBeHardDeleted,false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstRequestToBeHardDeleted);
                }
                //Database.emptyRecycleBin(lstRequestToBeHardDeleted);
                Logger.info('Requests are deleted.');
            }
           
            //Query Opportunities to be deleted
            List<Opportunity> lstOpportunities = [Select id, AccountId, Account.DPA_Status__c from Opportunity where AccountId in: setAccId];
            List<Opportunity> lstOppsToBeDeleted = new List<Opportunity>();
           
            for(Opportunity opp : lstOpportunities){
                if(opp.Account.DPA_Status__c == sixPlus){
                    lstOppsToBeDeleted.add(opp);
                }
            }
            if(!lstOppsToBeDeleted.isEmpty()){
                Database.delete(lstOppsToBeDeleted,false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstOppsToBeDeleted);
                }
                //Database.emptyRecycleBin(lstOppsToBeDeleted);
                Logger.info('Opportunities are deleted');
            }
            //Query Accounts to be deleted
            List<Account> lstAcc = [SELECT Id, DPA_Status__c FROM Account
                                    WHERE Id in: setAccId
                                    ORDER BY Id];
          system.debug('lstAcc>>'+lstAcc);
            List <Account> lstAccToBeDeleted = new List <Account>();
            for(Account acc : lstAcc){
                if(setAccId.contains(acc.Id) && acc.DPA_Status__c == SixPlus) {
                    lstAccToBeDeleted.add(acc);
                }
            }
            //WHCRM Start-81387-Query Account related Contracts to be deleted
           
            List<Contract> lstContract = [SELECT Id,Account.DPA_Status__c FROM Contract WHERE Accountid In: lstAccToBeDeleted];    
           
            if(!lstContract.isEmpty()){
                Database.delete(lstContract,false);
             if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
            {
                Database.emptyRecycleBin(lstContract);
            }
            //Database.emptyRecycleBin(lstCaseToBeDeleted);
            Logger.info('Contracts are deleted.');  
            system.debug('lstAccToBeDeleted>>'+lstAccToBeDeleted);
            if(!lstAccToBeDeleted.isEmpty()){
                Database.delete(lstAccToBeDeleted,false);
                if(FeatureManagement.checkPermission(Constants.sIntegration_Cust_Per))
                {
                    Database.emptyRecycleBin(lstAccToBeDeleted);
                }
                //Database.emptyRecycleBin(lstAccToBeDeleted);
                Logger.info('Accounts are deleted');
            }
        } //END WHCRM-81387
       
        }
       
 catch(exception e){
        System.debug('The following exception has occurred: ' + e.getMessage());
        Logger.error('Error: The following error has occured: '+ e.getMessage() + ' ' + e.getStackTraceString());
    }
}
   
global void finish(Database.BatchableContext BC){
   
}

}
