package database

type AccountCostRepository interface {
	List()
	Save()
}
