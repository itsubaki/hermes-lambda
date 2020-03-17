package database

type AccountCostRepository struct {
	Handler
}

func NewAccountCostRepository(h Handler) *AccountCostRepository {
	return &AccountCostRepository{
		Handler: h,
	}
}

func (r *AccountCostRepository) List() {

}

func (r *AccountCostRepository) Save() {

}
