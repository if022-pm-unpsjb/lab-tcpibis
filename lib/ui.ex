defmodule Libremarket.Ui do

  def comprar(id_producto, medio_de_pago, forma_de_entrega) do
    Libremarket.Compras.Server.comprar(id_producto, forma_de_entrega, medio_de_pago)
    #Libremarket.Compras.Server.confirmar_compra
  end
#modulo y funciones wue no ejecutan como un servidor solo llaman a otras funciones
#selecciona el producto, la forma de entrega y el medio de pago llamando a los servidores

end
