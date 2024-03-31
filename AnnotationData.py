class AnnotationData:
    def __init__(self, annotationNumber=0, frame=0, name="", annotation=""):
        self.annotationNumber = annotationNumber
        self.frame = frame
        self.name = name
        self.annotation = annotation

    def read(self, message):
        # Assuming 'message' is a dictionary with keys corresponding to the struct's fields,
        # this method would extract these values and assign them to the instance variables.
        self.annotationNumber = message.get('annotationNumber', 0)
        self.frame = message.get('frame', 0)
        self.name = message.get('name', "")
        self.annotation = message.get('annotation', "")
