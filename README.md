# entx

// TODO:
// - execution manuel des policy dans les search au moment du build
// - faire un build des scalars pour également appliquer les policy avant l'execution des query
// - faire passer dans le context une structure permettant de mofifier les scalar queries et donner des infos pour les policy
//
// si scalar (count ou aggregate overall) -> un appel aux policy:
// validation au build et peu modifier le sql.Selector
//
// si autre, deux type d'appel aux policy:
// 1. validation avant query (peu modifier le query?)
// 2. exec natif de ent avant le query et peu le modifier