cdef class OrgInfo:
    cdef public str org_id
    cdef public str changed
    cdef public str org_name
    cdef public str country
    cdef public str source
    
    cpdef dict asdict(self)

cdef class ASInfo:
    cdef public int aut
    cdef public str changed
    cdef public str aut_name
    cdef public str org_id
    cdef public str source
    
    cpdef dict asdict(self)

cdef class PotarooInfo:
    cdef public int aut
    cdef public str aut_name
    cdef public str name
    cdef public str country
    cdef public str url
    
    cpdef dict asdict(self)


cdef class Info:
    cdef public ASInfo asinfo
    cdef public OrgInfo orginfo
    cdef public PotarooInfo potarooinfo

cdef class AS2Org(dict):
    cdef public dict data
    cpdef Info info(self, int asn)
    cpdef str name(self, int asn)