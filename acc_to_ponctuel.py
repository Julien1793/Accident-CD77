# -*- coding: cp1252 -*-
#-------------------------------------------------------------------------------
# Name:        G�n�ration des accidents en ponctuel
# Author:      J Samaniego
# Created:     22/07/2019
# Contenu :
# Le script g�n�re la donn�e accident en ponctuel sur une plage d�ann�e choisie
# avec les champs de la table accident (caract�ristique) et la table lieux souhait�s.
# Ce script calcule aussi les accidents en agglom�ration et le niveau de SDOR de
# l�accident. Attention, la classe d�entit� ponctuelle en sortie comporte des
# accidents sans g�om�trie car ils ne sont pas localisables sur le r�f�rentiel
# routier. Un champ � LOCALISATION � permet de les identifier.
#-------------------------------------------------------------------------------

import arcpy,os,sys
from datetime import datetime

##sys.path.append(r'K:\SIG\DPR\08_OUTILS\8.6_SCRIPT\PYTHON\_Biblio_DPR')
##import Route77

sys.path.append (r'K:\SIG\DPR\08_OUTILS\8.6_SCRIPT\PYTHON\Traitement_des_donnees_thematiques\Agglo')
import CleanAgglo

def TRACE(txt):
    print(txt)
    arcpy.AddMessage(txt)

def ALERTE(txt):
    print(txt)
    arcpy.AddWarning(txt)

def ERREUR(txt):
    print "ERREUR : " + txt
    arcpy.AddError(txt)
    sys.exit ()

def CLEAN_FIELDS (laTable,lesChampsOk):
    """
    Supprime les cahmps de la table � l'execption de ceux de la liste en entr�e
    et de ceux indiqu� comme obligatoire dans la structure de la table (champs
    syst�mes de la donn�es objectid, shape, ...)
    """
    lesChamps = []
    for f in arcpy.ListFields(laTable):
        arcpy.SetProgressorLabel("CleanField : {}".format(f.name))
        if not f.required:
            # �limine les champs syst�mes.
            lesChamps.append(str(f.name))
            if not f.name in lesChampsOk:
                arcpy.DeleteField_management(laTable,f.name)

    for champ in lesChampsOk:
        if not champ in lesChamps:
            arcpy.AddWarning("le champs {} n'existe pas dans la {}".format(champ,laTable))

##def VerifExistance(laData):
##    if not arcpy.Exists(laData):
##        ALERTE("\t - " + laData + "' n'existe pas ! ")
##        sys.exit()
##    else:
##        leDesc = arcpy.Describe(laData)
##        TRACE("\t - " + str(leDesc.dataType) + " - " + str(laData) + "' : OK")
##
##def VerifAbsence(laData):
##    if arcpy.Exists(laData):
##        ERREUR("\t - " + str(laData) + "' existe d�j� ! ")
##        sys.exit()
##    else:
##
##        TRACE("\t - " + str(laData) + "' : A cr�er")

arcpy.env.overwriteOutput = False

def PreparationAcc(laGdbW,laT_Acc,lesChampsAcc,laT_Lieu,lesChampLieu,AN_MAX,AN_MIN,laT_Agglo,laT_Sdor,leRouteM):
#--------------------------------------------Les variables du traitement--------------------------------------------------
# ----- Vairables principales du traitement
    Vue_acc_w       = "Vue_acc"
    Vue_lieux_w     = "Vue_lieux"
    Evt_acc_lieux   = "Event_acc_lieux_point"
    Nom_acc_lieux   = "PT_ACCIDENT_{}_SUR_{}ANS".format (AN_MAX,(AN_MAX-AN_MIN)+1)
    Pt_acc_lieux    = laGdbW + os.sep + Nom_acc_lieux
    lenomT1         = "ACC_LIEUX_{}_SUR_{}ANS_TAB".format(AN_MAX,((AN_MAX-AN_MIN)+1))
    lenomT2         = "ACC_ACCIDENT_{}_SUR_{}ANS_TAB".format(AN_MAX,(AN_MAX-AN_MIN)+1)
    Nom_acc         = "ACCIDENT_TAB_FINAL"
    laT_acc         = laGdbW + os.sep + Nom_acc
    laT_acc_x_agglo = laGdbW + os.sep + "ACCIDENT_X_AGGLO"
    laT_acc_x_Sdor  = laGdbW + os.sep + "ACCIDENT_X_SDOR"
#--------------------------------------------Import des donn�es soucres dans la gdb de travail--------------------------------------------------
##    VerifAbsence (Pt_acc_lieux)
    exp_tab=[laT_Acc,laT_Lieu,laT_Sdor]
    lesTableImportNom = []
    listF_acc_oblige = ["ID_ACCIDENT","C_AN","T_NB_TOTAL_TUES","T_NB_TOTAL_BG","T_NB_TOTAL_BL"]
    listF_lieux_oblige = ["ID_ACCIDENT","CG_ROUTE","CG_CUMUL"]
    TRACE ("Contr�le des champs obligatoires des tables accident et lieux pour les traitements")
    for f in listF_acc_oblige:
        if f not in lesChampsAcc:
            lesChampsAcc.append (f)
    for f in listF_lieux_oblige:
        if f not in lesChampLieu:
            lesChampLieu.append (f)

    TRACE ("Import des donn�es sources :")
    if AN_MIN == 0 :
        AN_MIN = min([int(row.C_AN) for row in arcpy.SearchCursor(laT_Acc)])
        del row
    for t in exp_tab:
        nom = os.path.split(t)[1] ##.replace(".","_")
        wc=""
        if nom[:6] == "ROUTE.":
            nom= nom[6:]

        if nom == "ACC_ACCIDENTS_TAB":
            nom ="ACC_ACCIDENT_{}_SUR_{}ANS_TAB".format(AN_MAX,(AN_MAX-AN_MIN)+1)
            if AN_MIN == 0:
                wc= "C_AN <= {}".format (AN_MAX)
            else:
                wc="C_AN>= {} AND C_AN<= {}".format (AN_MIN,AN_MAX)
        if nom == "ACC_LIEUX_TAB":
            nom="ACC_LIEUX_{}_SUR_{}ANS_TAB".format(AN_MAX,((AN_MAX-AN_MIN)+1))
            if AN_MIN == 0:
                wc= "ROUTE_AFFECT = '1' AND DATE_ACCIDENT <= timestamp '{}-12-31 00:00:00'".format (AN_MAX)
            else:
                wc="ROUTE_AFFECT = '1' AND (DATE_ACCIDENT >= timestamp  '{}-01-01 00:00:00' AND DATE_ACCIDENT <= timestamp '{}-12-31 00:00:00')".format (AN_MIN,AN_MAX)

        lesTableImportNom.append(nom)
        TRACE ("\t- {}".format (nom))
        arcpy.TableToTable_conversion (t,laGdbW, nom, wc)

    laT_AggloW = laGdbW + os.sep+ "AGGLO_TAB_CLEAN"
    TRACE ("\t- {}".format (os.path.split(laT_AggloW)[1]))
    if arcpy.Exists (laT_AggloW):
        arcpy.Delete_management (laT_AggloW)
    CleanAgglo.aggloClean(laGdbW,laT_Agglo)

#--------------------------------------------TRAITEMENTS--------------------------------------------------
# ----- D�finition des tables de travail issues de l'import
    laT_AccW   = laGdbW + os.sep + lesTableImportNom[0]
    laT_LieuW  = laGdbW + os.sep + lesTableImportNom[1]
    laT_SdorW  = laGdbW + os.sep + lesTableImportNom[2]
    listDataTemp    = [laT_AccW,laT_LieuW,laT_AggloW,laT_SdorW]

# ----- Nettoyage des champs de la table accident et lieux
    TRACE ("\t- Nettoyage des champs {}".format (os.path.split(laT_AccW)[1]))
    CLEAN_FIELDS (laT_AccW,lesChampsAcc)
    TRACE ("\t- Nettoyage des champs {}".format (os.path.split(laT_LieuW)[1]))
    CLEAN_FIELDS(laT_LieuW,lesChampLieu)

# ----- Cr�ation de la classe d'entit� ponctuelle accident
    TRACE ("G�n�ration des accidents en ponctuel :")
    arcpy.MakeTableView_management (laT_AccW,Vue_acc_w)
    listDataTemp.append (Vue_acc_w)
    arcpy.MakeTableView_management (laT_LieuW,Vue_lieux_w)
    listDataTemp.append (Vue_lieux_w)
    TRACE ("\t- Jointure {} et {}".format (os.path.split(laT_LieuW)[1],os.path.split(laT_AccW)[1]))
    arcpy.AddJoin_management (Vue_lieux_w,"ID_ACCIDENT",Vue_acc_w,"ID_ACCIDENT","KEEP_COMMON")
    TRACE ("\t- Cr�ation de l'�v�nnement avec jointure en ponctuel")
    arcpy.MakeRouteEventLayer_lr (in_routes= leRouteM,route_id_field ="ROUTE",
                              in_table= Vue_lieux_w,in_event_properties= "{}.CG_ROUTE POINT {}.CG_CUMUL".format (os.path.split(laT_LieuW)[1],os.path.split(laT_LieuW)[1])
                              ,out_layer = Evt_acc_lieux)
    listDataTemp.append (Evt_acc_lieux)
    TRACE ("\t- Transformation en classe d'entit� point : cr�ation {}".format (Nom_acc_lieux))
    arcpy.FeatureClassToFeatureClass_conversion (Evt_acc_lieux,laGdbW,Nom_acc_lieux)
    TRACE ("\t- Renommage des champs de {}".format (Nom_acc_lieux))
    lesnewChamps = []
    for f in arcpy.ListFields(Pt_acc_lieux):
        if f.required:
            lesnewChamps.append (f.name)
        else:
            if lenomT1 == f.name[:len(lenomT1)]:
                newName = f.name[(len(lenomT1)+1):]
                if newName in lesnewChamps or newName in [c.name for c in arcpy.ListFields(Pt_acc_lieux)]:
                    newName = newName + "_1"
                arcpy.AlterField_management (in_table= Pt_acc_lieux,field= f.name,new_field_name= newName)
                lesnewChamps.append (newName)
            elif  lenomT2 == f.name[:len(lenomT2)]:
                newName = f.name[(len(lenomT2)+1):]
                if newName in lesnewChamps or newName in [c.name for c in arcpy.ListFields(Pt_acc_lieux)]:
                    newName = newName + "_1"
                arcpy.AlterField_management (in_table= Pt_acc_lieux,field= f.name,new_field_name= newName)
                lesnewChamps.append (newName)

            else:
                lesnewChamps.append (f.name)
    dropfields = ["OBJECTID_1", "ID_ACCIDENT_1"]
    arcpy.DeleteField_management (Pt_acc_lieux, dropfields)
    TRACE("\t- Calcul des bless�s au total (graves + l�gers)")
    arcpy.AddField_management (Pt_acc_lieux,"TOTAL_BLESSES","SHORT")
    arcpy.CalculateField_management (Pt_acc_lieux,"TOTAL_BLESSES","[T_NB_TOTAL_BG]+[T_NB_TOTAL_BL]")

# ----- Identification des accidents en agglom�ration
    TRACE("Identification des accidents en agglomeration :")
    TRACE ("\t- Superpostion d'itin�raire accident et agglo")
    arcpy.TableToTable_conversion (Pt_acc_lieux,laGdbW,Nom_acc)
    listDataTemp.append (laT_acc)
    arcpy.OverlayRouteEvents_lr (laT_acc,in_event_properties= "CG_ROUTE POINT CG_CUMUL",overlay_table=laT_AggloW,overlay_event_properties="ROUTE LINE CUMULD CUMULF",
                             overlay_type="INTERSECT",out_table= laT_acc_x_agglo,out_event_properties="CG_ROUTE POINT CG_CUMUL",  zero_length_events="NO_ZERO",
                             in_fields="FIELDS",build_index="NO_INDEX")
    listDataTemp.append (laT_acc_x_agglo)
    TRACE ("\t- R�cup�ration du champ des accidents en agglo")
    arcpy.JoinField_management (Pt_acc_lieux,"ID_ACCIDENT",laT_acc_x_agglo,"ID_ACCIDENT","EN_AGGLO")

# ----- Identification du SDOR des accidents
    TRACE ("Identification du SDOR :")
    TRACE ("\t- Superposition table ACCIDENT et SDOR")
    arcpy.OverlayRouteEvents_lr (laT_acc,in_event_properties= "CG_ROUTE POINT CG_CUMUL",overlay_table=laT_SdorW,overlay_event_properties="ROUTE LINE CUMULD CUMULF",
                             overlay_type="INTERSECT",out_table= laT_acc_x_Sdor,out_event_properties="CG_ROUTE POINT CG_CUMUL",  zero_length_events="NO_ZERO",
                             in_fields="FIELDS",build_index="NO_INDEX")
    listDataTemp.append (laT_acc_x_Sdor)
    TRACE ("\t- R�cup�ration du champ ENJEU du SDOR")
    arcpy.JoinField_management (Pt_acc_lieux,"ID_ACCIDENT",laT_acc_x_Sdor,"ID_ACCIDENT","ENJEU")

# ----- Identification des accidents sans g�om�trie
    arcpy.AddField_management (Pt_acc_lieux,"LOCALISATION","SHORT")
    rows = arcpy.da.UpdateCursor (Pt_acc_lieux,["SHAPE@X","LOCALISATION"])
    for row in rows:
        if row[0] == None:
            row[1] = 0
        else :
            row[1] = 1
        rows.updateRow(row)

    del row,rows

    TRACE("Suppression des traitements interm�diaires")
    for t in listDataTemp:
        if arcpy.Exists (t):
            arcpy.Delete_management (t)
    TRACE ("Termin�")

#--------------------------------------------LES VARIABLES ARCGIS--------------------------------------------------

if __name__ == '__main__':
    Test = False
    if Test == True :
        laGdbW = r''

        laT_Acc = r''
        laChaineChampAcc = "ID_ACCIDENT;CG_INSEE"
        lesChampsAcc = laChaineChampAcc.split(";")

        laT_Lieu = r''
        laChaineChampLieu = "ID_ACCIDENT;ID_LIEU;CG_PR"
        lesChampLieu = laChaineChampLieu.split(";")

        laT_Agglo = r''
        laT_Sdor = r''
        leRouteM = r''

    if Test == False :
        laGdbW            = arcpy.GetParameterAsText(0) # Chemin de la gdb de travail

        laT_Acc           = arcpy.GetParameterAsText(1) # Table accident (caract�ristiques) de la base de donn�es accident SDE
        laChaineChampAcc  = arcpy.GetParameterAsText(2) # Choix des champs de la table accident � conserver
        lesChampsAcc      = laChaineChampAcc.split(";")

        laT_Lieu          = arcpy.GetParameterAsText(3) # Table lieux de la base de donn�es accident SDE
        laChaineChampLieu = arcpy.GetParameterAsText(4) # Choix des champs de la table lieux � conserver
        lesChampLieu      = laChaineChampLieu.split(";")

        AN_MAX            = arcpy.GetParameter (5) # Ann�e max des accdients � r�cup�rer
        AN_MIN            = arcpy.GetParameter (6) # Ann�e mini  des accidents � r�cup�rer

        laT_Agglo         = arcpy.GetParameterAsText(7) # Table des section en agglom�ratio SDE
        laT_Sdor          = arcpy.GetParameterAsText(8) # Table du SDOR (hi�rarchisation route) SDE
        leRouteM          = arcpy.GetParameterAsText(9) # R�f�rentiel routier SDE

    PreparationAcc(laGdbW,laT_Acc,lesChampsAcc,laT_Lieu,lesChampLieu,AN_MAX,AN_MIN,laT_Agglo,laT_Sdor,leRouteM)
