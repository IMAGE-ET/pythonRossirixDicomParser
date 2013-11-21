import dicom
import pymongo
import datetime
from pymongo import MongoClient

#['AccessionNumber', 'AcquisitionDate', 'AcquisitionDateTime', 'AcquisitionNumber', 'AcquisitionTime', 'AdmissionID', 'BitsAllocated', 'BitsStored', 'BodyPartExamined', 'CTDIvol', 'CTExposureSequence', 'Columns', 'ContentDate', 'ContentTime', 'ConvolutionKernel', 'CurrentPatientLocation', 'DataCollectionDiameter', 'DerivationCodeSequence', 'DerivationDescription', 'EstimatedDoseSaving', 'Exposure', 'ExposureModulationType', 'ExposureTime', 'FilterType', 'FrameOfReferenceUID', 'GantryDetectorTilt', 'HighBit', 'ImageComments', 'ImageOrientationPatient', 'ImagePositionPatient', 'ImageType', 'InstanceAvailability', 'InstanceCreationDate', 'InstanceCreationTime', 'InstanceNumber', 'InstitutionAddress', 'InstitutionName', 'InstitutionalDepartmentName', 'IssuerOfPatientID', 'KVP', 'Laterality', 'LossyImageCompression', 'LossyImageCompressionMethod', 'LossyImageCompressionRatio', 'Manufacturer', 'ManufacturerModelName', 'MedicalAlerts', 'ModalitiesInStudy', 'Modality', 'NameOfPhysiciansReadingStudy', 'OperatorsName', 'OtherPatientIDs', 'PatientAge', 'PatientBirthDate', 'PatientBirthTime', 'PatientID', 'PatientName', 'PatientPosition', 'PatientSex', 'PerformedProcedureStepID', 'PerformingPhysicianName', 'PhotometricInterpretation', 'PixelData', 'PixelRepresentation', 'PixelSpacing', 'PositionReferenceIndicator', 'PreMedication', 'ProtocolName', 'ReconstructionDiameter', 'RefdImageSequence', 'ReferencedImageSequence', 'ReferringPhysicianName', 'RequestedContrastAgent', 'RequestingService', 'RescaleIntercept', 'RescaleSlope', 'RotationDirection', 'Rows', 'SOPClassUID', 'SOPInstanceUID', 'SamplesPerPixel', 'ScanOptions', 'SeriesDate', 'SeriesDescription', 'SeriesInstanceUID', 'SeriesNumber', 'SeriesTime', 'SliceLocation', 'SliceThickness', 'SoftwareVersions', 'SpacingBetweenSlices', 'SpecificCharacterSet', 'StationName', 'StudyComments', 'StudyDate', 'StudyDescription', 'StudyID', 'StudyInstanceUID', 'StudyTime', 'TableHeight', 'VariablePixelData', 'WindowCenter', 'WindowWidth', 'XRayTubeCurrent']

def serializeImage(ds,xid):
    img={}
    img['xid']=xid
    img['instanceNumber']=ds.get("InstanceNumber")

    return img;



def parsePatient(ds):

    patient={}
    patient['sex']=ds.get('PatientSex',None)
    patient['name']=ds.get('PatientName',None)
    patient['dateOfBirth']=ds.get('PatientBirthDate',None)
    #calcular edad
    patient['age']=ds.get('PatientAge',None)
    patient['id']=ds.get('PatientID',None)

    return patient


def serializeSeries(ds,xid,studyUID):
    s={}
    s['xid']=xid
    s['name']=ds.get('SeriesDescription',None)
    s['description']=ds.get('SeriesDescription',None)
    s['frameCount']=-1
    s['studyInstanceUID']=studyUID
    s['modality']=ds.get('Modality',None)
    
    
    return s;


def serializeStudy(ds,studyUID):

    study={}
    
    study['name']=ds.get('ProtocolName',None)
    date=ds.get('StudyDate',None)

    
    year=int(date[0:4])
    month=int(date[4:6])
    day=int(date[6:8])
    print year
    print month
    print day

    d = datetime.datetime(year,month,day)

    study['date']=d
    study['accessionNumber']=ds.get('AccessionNumber',None)
    study['modality']=ds.get('Modality',None)
    study['referringPhysician']=ds.get('ReferringPhysicianName',None)
    study['performingPhysician']=ds.get('PerformingPhysicianName',None)
    study['studyInstanceUID']=studyUID
    study['institutionName']=ds.get('InstitutionName',None)
    study['patient'] = parsePatient(ds)




    try:
        study["orderNumber"]= ds[0x0040,0x1007].value
    except KeyError:
        None

    try:        
        study["institutionAddress"]= ds[0x00080,0x0081].value
    except KeyError:
        None

    try:
        study["PerformedProcedureStepDescription"]= ds[0x0040,0x0254].value
    except KeyError:
        None

    try:        
        study["PerformedStationAETitle"]= ds[0x0040,0x0241].value
    except KeyError:
        None

    try:        
        study["StationName"]= ds[0x0008,0x1010].value
    except KeyError:
        None

    try:
        study["CurrentPatientLocation"]= ds[0x0038,0x0300].value
    except:
        None

    try:
        study["OtherPatientIDs"]= ds[0x0010,0x1000].value
    except:
        None

    try:
        study["ReferringLicense"]= ds[0x0008,0x1050].value
    except:
        None

    return study

def main():
    mongo=MongoClient()
    studies=mongo['rossirix']['studies']
    series=mongo['rossirix']['series']
    dicomList=open('/Users/dnul/RepoAntena/pluginOutput.txt')
    for line in dicomList:
        [imageXID,seriesXID,studyUID,path]=line.split(',')
        path=path.strip('\n')
        ds = dicom.read_file(path)
        st=studies.find_one({'studyInstanceUID':studyUID})
        if st==None:
            #insert study
            studies.insert(serializeStudy(ds,studyUID))
        

        ser = serializeSeries(ds,seriesXID,studyUID)
        img = serializeImage(ds,imageXID)
        series.update({'xid':seriesXID},{'$push': { 'frames': img },'$set':ser},upsert=True,multi=False)

        #{ xid: "517DEE8C-21C2-434B-B9EE-CB8DD52AF267/Series/p1398883" } update: { $push: { frames: { xid: "517DEE8C-21C2-434B-B9EE-CB8DD52AF267/Image/p41894359", instanceNumber: 316 } }, $set: { studyInstanceUID: "1.2.840.113704.1.111.1448.1384953147.1", modality: "CT", frameCount: 432, name: "unnamed", description: "TX ABD Y PEL C-C-Abdomen", xid: "517DEE8C-21C2-434B-B9EE-CB8DD52AF267/Series/p1398883" } }
        studies.update({'studyInstanceUID':studyUID},{'$addToSet':{'series':ser}},multi=False)
        #ds = dicom.read_file(line)
        #print serializeStudy(ds)
        #print serializeSeries(ds)
        #print serializeImage(ds)
    
    


main()