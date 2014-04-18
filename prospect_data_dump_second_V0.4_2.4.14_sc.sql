
with 
 pw_leases as 
   (select pw_lease.*
     ,replace(replace(replace(replace(pw_lease.primarycontacthomephone,' ',''),'-',''),'(',''),')','') as home_phone
     ,replace(replace(replace(replace(pw_lease.primarycontactmobile,' ',''),'-',''),'(',''),')','') as mobile_phone
     ,replace(replace(replace(replace(pw_lease.primarycontactworkphone,' ',''),'-',''),'(',''),')','') as work_phone
     ,(case when length(replace(replace(replace(replace(pw_lease.primarycontacthomephone,' ',''),'-',''),'(',''),')','')) >=10 
                 then replace(replace(replace(replace(pw_lease.primarycontacthomephone,' ',''),'-',''),'(',''),')','') else '' end) || 
      (case when length(replace(replace(replace(replace(pw_lease.primarycontactmobile,' ',''),'-',''),'(',''),')','')) >=10
                 then replace(replace(replace(replace(pw_lease.primarycontactmobile,' ',''),'-',''),'(',''),')','') else '' end) ||
      (case when length(replace(replace(replace(replace(pw_lease.primarycontactworkphone,' ',''),'-',''),'(',''),')','')) >= 10
                 then replace(replace(replace(replace(pw_lease.primarycontactworkphone,' ',''),'-',''),'(',''),')','') else '' end) as combined_phone
    from pw_lease)

SELECT
   prospect_data.*
  ,secondary.*
  ,pw_building.portfolioabbreviation ||' | '||pw_building.buildingabbreviation||' | '||prospect_data.desired_unit_name as desired_location
  ,prospect_data.master_lead_provider
  ,prospect_data.approval_type as approval_type_clean
  ,prospect_data.missed_showing_count as no_show_clean
  ,prospect_data.cancelled_showing_count as cancelled_clean
  ,case when (prospect_data.total_showing_count !='' and cast(prospect_data.total_showing_count as numeric) > 0) or
              prospect_data.application_submitted_on !='' then '1' else '0' end as had_showing_set
  ,case when prospect_data.total_showing_count ='' then '0' else prospect_data.total_showing_count end as total_showings_set
  ,case when prospect_data.pending_showing_count ='' then '0' else prospect_data.pending_showing_count end as pending_showing
  ,case when prospect_data.missed_showing_count ='' then '0' else prospect_data.missed_showing_count end as no_show
  ,case when prospect_data.cancelled_showing_count ='' then '0' else prospect_data.cancelled_showing_count end as cancelled
  ,case when prospect_data.rescheduled_showing_count ='' then '0' else prospect_data.rescheduled_showing_count end as rescheduled
  ,case when prospect_data.shown_showing_count ='' then '0' else prospect_data.shown_showing_count end as show
--   ,'' as showing_status
  ,case when prospect_data.pending_showing_count not in('','0.0') then 'Pending' else 'N/A' end as showing_pending
  ,''as showing_set_by_clean
  ,case when prospect_data.application_submitted_on !='' then '1' else '0' end as application_in
  ,'' as app_status_dirty
  ,case when prospect_data.decision like '%Approved%' and prospect_data.decision not like '%Pending%' then 'Approved' else prospect_data.decision end as app_status
  ,case when prospect_data.decision like '%Approved%' and prospect_data.decision not like '%Pending%' then '1' else '0' end as approved
  ,case when pw_leases.leasename is not null then '1' else '0' end as converted
  ,case when prospect_data.lease_signing_count != '' then '1' else '0' end as lease_signing_set
  ,'' as created_to_showing
  ,'' as showing_to_app
  ,'' as app_to_converted
  ,case when pw_building.portfolioabbreviation is null then 'N/A' else pw_building.portfolioabbreviation end as fund
  ,case when pw_building.buildingabbreviation is null then 'N/A' else pw_building.buildingabbreviation end as building
  ,case when prospect_data.desired_unit_name ='' then 'N/A' else prospect_data.desired_unit_name end as unit
  ,case when pw_building.zonename is null then 'N/A' else pw_building.zonename end as zone
  ,case when pw_building.zonepm is null then 'N/A' else pw_building.zonepm end as pm
  ,case when pw_building.leasingagent is null then 'N/A' else pw_building.leasingagent end as leasing_agent
  ,case when pw_building.zonename in('ZONE 20','ZONE 21') then 'Indianapolis'
        when pw_building.zonename in('ZONE 30','ZONE 31') then 'Baltimore'
        when pw_building.zonename in('ZONE 1','ZONE 2','ZONE 3','ZONE 4','ZONE 5') then 'Chicago'
        else 'N/A' end as market
  ,'' as employee
  ,pw_building.buildingabbreviation||prospect_data.desired_unit_name as building_unit_uid
  ,prospect_data.target_rent
  ,case when prospect_data.max_rent = '' or prospect_data.target_rent='' then 0.0 else cast(prospect_data.max_rent as numeric) - cast(prospect_data.target_rent as numeric) end as rent_shortfall
  ,prospect_data.prospect_created_quarter as quarter
  ,case when prospect_data.sub_lead_provider ='' then 'none' else prospect_data.sub_lead_provider end as sub_lead_clean
  ,case when prospect_data.sub_lead_provider in('ADWLP','BDSADW','CPSADW','GA250','GA350','GA450','GAAS8','GAGEN','GAUBN','GAUBN8','GCHAT',
                                                'GCHAT8','GCHI','GCHI8','GCHIB','GCHIC','GCHILP','GCREDIT','GIND','GIND8','GINDB','GINDC',
                                                'GNHS','GNHS8','GOOG','Google','GPANLP','GPF','GPF8','GSECTLP','GSS','GSS8','LANDADW','LOCCADW',
                                                'LOCSADW','LOWLOCADW ','MADW','P2CADW','PCADW','PSADW','SECSADW','STHSADW','WSTSADW ','SCOLLEGE',
                                                'MARIAN','GBRAND','GCOMP','GCONTS8','GZONE1','GZONE2','GZONE3','GZONE4','GZONE5','GZONE20','GZONE21'
                                                ) then 'Adwords'
       when prospect_data.sub_lead_provider in('CATH','FFIST','HCP','HEART','INNER','INSPIR','UCAN') then 'Agency'
       when prospect_data.sub_lead_provider in('AGB','FRB') then 'AptBooks'
       when prospect_data.sub_lead_provider in('BING','BINGC8','PFBING','BZONE1','BZONE2','BZONE3','BINGBRAND','BINGBRAND') then 'BING'
       when prospect_data.sub_lead_provider in('CRAI','CRAIAG','CRAIE','CBT','CRAIN','CIT','CMY','CAG','CAS','CMYTST','CAGTST','CS8','CTW',
                                               'CAT','CMT','CLB','CPW','CGR','CRL','CVL','CCL','CHL','CML','CVY','CFL','CCE','CTT','CF2','CAH','CA2') then 'Craigslist'
                                               
       when prospect_data.sub_lead_provider in('74','77','B103','B742','B774','BCHI','BCTA74','BG74','BG77','BGCHI','BSAD77','BUS','BUS13',
                                               'BUS77','CTA77','GLCTA','GLH9','GLINE3','GREEN','GREENRED','GRN4','LGRN8','LINE','RAIL','RCTA',
                                               'RDL3','RDLSPRING','RED','REDAD','REDH9','REDL4','REDWIN12','RLINE2','RLN8','STOP','TRAIN',
                                               'HYDEPARK','REDLINE13','GREENLINE13') then 'CTA'
       when prospect_data.sub_lead_provider in('DIAL') then 'Dialer-Port'
       when prospect_data.sub_lead_provider in('EMAPTG','JM250','JM324','JM350','JM424','JM450','JME282','LFPRE') then 'Email-ACQ'
       when prospect_data.sub_lead_provider in('APP2100','APP4250','APPCTA3','HEAT25','MORE100','NEW25','REAC224','RLU2','RLU3','RLU3',
                                               'SHOWA100','SHOWA25','SPECIALS25','VALUE25','WORD') then 'Email-Port'
       when prospect_data.sub_lead_provider in('FBOOK','FACEBOOK') then 'Facebook'
       when prospect_data.sub_lead_provider in('SEC8LIST','S8QM','GSEC8') then 'GSEC8'
       when prospect_data.sub_lead_provider in('GSWAG','PWHO','VINEHO','CTSHO','HILLHO','GROHO','VISHO','CEDHO','RIVHO','GINFLY',
                                               'SPINGHO','GRCOL','RICOL','IHF') then 'Guerilla'
       when prospect_data.sub_lead_provider in('TISD') then 'IndyStar'
       when prospect_data.sub_lead_provider in('OODLE','YAHO') then 'Internet'
       when prospect_data.sub_lead_provider in('ACON','AHLS','APA','AXONAS','BHR','CAF','CHIA','CITYWIDE','DREAM','DRT','DWELL','ELAN','GOTV','HANSEN','HOUSE',
                                               'HPR','HURA','INFINITE','JABS','JADE','JCON','KALE','KARBON','KELLER','KELLY','LOOP','LPRO','MALLARD',
                                               'MREALTY','MYAPT','NATI','NCMBC','NGATE','OLAD','PROEQ','R2R','REFUGE','REMAX','RENEW','RKP','ROCK',
                                               'ROYAL','RPM','RPREALTY','RUFF','SPU','TIMBER','THRESHOLDS','TNR','TPH','TROCK','UASP','URENT','YVET','WEP','CASS',
                                               'WUBB','AMERICAN','PSTREET','WINNIE','ATEAM','FULTON','URBAN') then 'LeasingAgency'
      when prospect_data.sub_lead_provider in('LACH1','LACH2','LACH3','LACH4','LACH5','LACH6','LACH7','LAB1','LAB2','LAB3','LAB4','LAIN1',
                                              'LAIN2','LAIN3','LAIN4','LAIN5','LAIN6','LAIN7') then 'LeasingAgent'
      when prospect_data.sub_lead_provider in('APAR','APTG','CHAW','CORT','DOMU','FORRENT','RENT','RNTLS','ALIST') then 'ListingSites'
      when prospect_data.sub_lead_provider in('GPZ1','GPZ2','GPZ3','GPZ4','GPPW','GPMD','GPCD','GPVY','GPRS','GPVS','GPHLS','GPGRV','GPPF','GPZ1',
                                              'GPZ2','GPZ3','GPZ4','GPZ30','GPCTS','BPZ30','BPRS','BPCD','BPVY','BPVT','BPHLS','BPCTS',
                                              'BPGRV','BPPW','BPZ1','BPZ2','BPZ3','BPZ4','BPPF','GPDREX') then 'Local'
      when prospect_data.sub_lead_provider in('APTFD','HOTP','MOVE','NEWP','RTHOME','GSEC8','ILIH','PADMAP','RBITS','RMINT','ZILLOW') then 'LS2'
      when prospect_data.sub_lead_provider in('M1250','M2559','M283','M3559','M446','M643','MAQE34','INDYESP','INDYMAIL') then 'Mail-ACQ'
      when prospect_data.sub_lead_provider in('MRE24') then 'Mail-Port'
      when prospect_data.sub_lead_provider in('MPAN23','MPAN24','MPAN34') then 'Mail-Port2'
      when prospect_data.sub_lead_provider in('MED') then 'Metra'
      when prospect_data.sub_lead_provider in('MOBILE') then 'MOBILE'
      when prospect_data.sub_lead_provider in('310HOUSE','ChathamT','COLES416','Coles7834','COLS7601','COLS7706','HOUSE','ING8155','INGS8051',
                                              'MAR8236','MHOUSE','OHCED','OHPW','PF0310','PF0602','PF3324','SShoreOH','SSOH','state7929','SV21746',
                                              'VAFAIR','Z4HOUSE','PF504','8127SE','PF518','CHAT518','PF601','CHAT608','OHFLYER','OH718','727OH',
                                              'KS727','SS727','727ST','OH713','801OH','OHSTR','FLYOH') then 'OpenHouse'
      when prospect_data.sub_lead_provider in('ATTPPC','Other') then 'Other'
      when prospect_data.sub_lead_provider in('INDB','IndyGO','TASTE','PACE') then 'OUTDOOR'
      when prospect_data.sub_lead_provider in('AUSW','BULL','CHIJ','DEFE','FRB','HYDE','LAVOZ','NICKEL','SKYL','SOUT','STAR','STOWN','TRIB',
                                              'VOIC','WIND','STAROL','DEXBOOK','FRIBC','FRVY','FRVS','FRPW','FRGV') then 'Publication'
      when prospect_data.sub_lead_provider in('1063','1390','923','WEDJ','RADIO','W107','WNTS','LALEY') then 'Radio'
      when prospect_data.sub_lead_provider in('PANR') then 'Referral'
      when prospect_data.sub_lead_provider in('GREM') then 'Remarketing'
      when prospect_data.sub_lead_provider in('PANGEARE','AGSEO','AUSTSEO','CHATAPT','CHATSEO','CHIAPT','CHICHEAP','CHILOW','CHISEC8','CHISEO',
                                              'INDYAPT','INDYCHEAP','INDYLOW','INDYSEC8','NHSAPT','NSEO','PFAPT','S8SEO','SHORESEO','SSHOREAPT',
                                              'STHSEO','ISEOCED','ISEOHLS','ISEOPW','ISEORS','ISEOVY','ISEOVST','ISEOCTS','ISEOGRV','CSEOPP',
                                              'CSEO','ISEO','BSEO','OKSITE','PNSITE','PWSITE','CDSITE','RSDSITE','HLSITE','MDSITE','VYSITE',
                                              'VSTSITE','CTSITE','GVSITE','FDSITE','PFTSITE') then 'SEO'
      when prospect_data.sub_lead_provider in('PANB','PANBSF','BAN2013','PFSIGN','RSSIGN','CDSIGN','VYSIGN','VTSIGN','HLSIGN','GRSIGN','PWSIGN',
                                              'CTSIGN','INDYFLYER','BALTFLYER','HOSTR','SSFEST','STSHIRT','INGLAWN','STREET','S8FLY','STHO') then 'Signage'                                    
      when prospect_data.sub_lead_provider in('STIMES','STLA','STOWN','SUN250','SUNC','SUNT','SUNT','SUNTSS') then 'SunTimes'
      when prospect_data.sub_lead_provider in('END','MID','REMSSF','RTXEM','RTXEM','SHOWTXT') then 'Text-Port'
      when prospect_data.sub_lead_provider in('AM26','BGC','BSOUTH','BSUB','CLTV','CWPP','CWPRE','EVE26','FOX','NEWS','SUNCW','TVPAN',
                                              'WCIU','WGN') then 'TV'         
      when lower(prospect_data.sub_lead_provider) in('walk') then 'Walk'
      when prospect_data.master_lead_provider ='' then 'none'
      else prospect_data.master_lead_provider end as master_lead_clean
  ,prospect_data.subsidy_type as public_assistance
  ,pw_leases.publicassistanceprogram as pap_clean
  ,case when prospect_data.subsidy_type !='' or pw_leases.publicassistanceprogram !='None' then 'yes' else 'no' end as subsidy_lead
  ,'' as conv_turnaround
  ,'no' as student
  ,case when prospect_data.sub_lead_provider in('ACON','AHLS','APA','BHR','CAF','CHIA','CITYWIDE','DREAM','DRT','DWELL','ELAN','GOTV','HANSEN',
                                               'HPR','HURA','INFINITE','JABS','JADE','JCON','KALE','KARBON','KELLER','KELLY','LOOP','LPRO',
                                               'MREALTY','MYAPT','NATI','NGATE','OLAD','PROEQ','R2R','REFUGE','REMAX','RENEW','RKP','ROCK',
                                               'ROYAL','RPM','RPREALTY','RUFF','SPU','THRESHOLDS','TNR','TPH','UASP','URENT','YVET','CASS',
                                               'WUBB') then 'yes' else 'no' end as agency
  --count(case when prospect_data.application_submitted_on !='' then 1 else null end) as applications 
  --count(case when pw_leases.leasename is not null or pw_leases_sec.leasename is not null then 1 else null end) as converted

FROM 
 --(select * from pcore_prospect_data where pcore_prospect_data.primary_or_secondary_applicant <> 'Secondary') as prospect_data

  (select prospect_data.*
          ,dups.id as dups_id
          ,dups.combined as dups_combined
   from (select * from pcore_prospect_data where pcore_prospect_data.primary_or_secondary_applicant <> 'Secondary') as prospect_data
   left outer join pcore_prospect_data as dups on prospect_data.id <> dups.id
                                         and dups.application_processed_on !=''
                                         and 
                                         ((length(prospect_data.combined) >= length(dups.combined)

                                         and ((length(dups.mobile_phone) >=10 and prospect_data.combined like '%'||dups.mobile_phone||'%')
                                             or (length(dups.home_phone) >=10 and prospect_data.combined like '%'||dups.home_phone||'%')
                                             or (length(dups.work_phone) >=10 and prospect_data.combined like '%'||dups.work_phone||'%')))
                                         or((length(prospect_data.combined) < length(dups.combined)
                                         and ((length(prospect_data.mobile_phone) >=10 and dups.combined like '%'||prospect_data.mobile_phone||'%')
                                             or (length(prospect_data.home_phone) >=10 and dups.combined like '%'||prospect_data.home_phone||'%')
                                             or (length(prospect_data.work_phone) >=10 and dups.combined like '%'||prospect_data.work_phone||'%')))))
                                         and prospect_data.first_name = dups.first_name
					 and prospect_data.last_name = dups.last_name
                                         /*
                                         and
                                        (((length(prospect_data.mobile_phone) >=10 and
				           dups.combined like '%'||prospect_data.mobile_phone||'%') or
                                            (length(dups.mobile_phone) >=10 and
				            prospect_data.combined like '%'||dups.mobile_phone||'%')) or

					((length(prospect_data.home_phone) >=10 and
					dups.combined like '%'||prospect_data.home_phone||'%') or
					(length(dups.home_phone) >=10 and
					prospect_data.combined like '%'||dups.home_phone||'%')) or

					((length(prospect_data.work_phone) >=10 and 
					dups.combined like '%'||prospect_data.work_phone||'%') or
					(length(dups.work_phone) >=10 and 
					prospect_data.combined like '%'||dups.work_phone||'%')) 

					and prospect_data.first_name = dups.first_name 
					and prospect_data.last_name = dups.last_name)
                                        */
    where dups.id is null) as prospect_data
 
  left outer join (select * 
                   from pcore_prospect_data
                   where pcore_prospect_data.primary_or_secondary_applicant = 'Secondary') as secondary on prospect_data.application_id = secondary.application_id

  left outer join pw_building on prospect_data.desired_building_name = pw_building.buildingabbreviation

  left outer join pw_leases on --Primary
                  (((length(prospect_data.mobile_phone) >=10 and
                  pw_leases.combined_phone like '%'||prospect_data.mobile_phone||'%') or
                   (length(prospect_data.home_phone) >=10 and
                   pw_leases.combined_phone like '%'||prospect_data.home_phone||'%') or
                   (length(prospect_data.work_phone) >=10 and 
                   pw_leases.combined_phone like '%'||prospect_data.work_phone||'%')) and
                  prospect_data.first_name = lower(pw_leases.primarycontactfirstname) and
                  prospect_data.last_name = lower(pw_leases.primarycontactlastname))
                  and cast(pw_leases.createdtime as date) >= cast(prospect_data.created as date) 
                  --or cast(pw_leases.createdtime as date) + interval '15 days' <= cast(prospect_data.created as date) )
                 /* or (((length(secondary.mobile_phone) >=10 or length(secondary.home_phone) >=10 or length(secondary.work_phone) >=10)
                   and pw_leases.combined_phone ~~* any(array['%'||secondary.mobile_phone||'%','%'||secondary.work_phone||'%','%'||secondary.home_phone||'%']))
                  and secondary.first_name = lower(pw_leases.primarycontactfirstname)
                  and secondary.last_name = lower(pw_leases.primarycontactlastname)
                  and cast(pw_leases.createdtime as date) >= cast(secondary.created as date))
*/
  left outer join pw_leases as pw_leases_sec on --Secondary
                  (((length(secondary.mobile_phone) >=10 and
                  pw_leases_sec.combined_phone like '%'||secondary.mobile_phone||'%') or
                   (length(secondary.home_phone) >=10 and
                   pw_leases_sec.combined_phone like '%'||secondary.home_phone||'%') or
                   (length(secondary.work_phone) >=10 and
                   pw_leases_sec.combined_phone like '%'||secondary.work_phone||'%')) and
                  secondary.first_name = lower(pw_leases_sec.primarycontactfirstname) and
                  secondary.last_name = lower(pw_leases_sec.primarycontactlastname))
                  and cast(pw_leases_sec.createdtime as date) >= cast(secondary.created as date)
                  --or cast(pw_leases_sec.createdtime as date) + interval '15 days' <= cast(secondary.created as date))

where 
 prospect_data.first_name != ''
 and prospect_data.last_name !=''
 and prospect_data.created >= '2013-10-01'

order by
 prospect_data.first_name
 ,prospect_data.last_name
 
